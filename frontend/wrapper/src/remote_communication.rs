//! Fork-owned relay client for the remote-control feature (web build only).
//!
//! Connects the running editor to the relay server as a WebSocket client, per the wire format in
//! the host repo's `design-spec/remote-control/wire-format.md`. This module owns all remote-control
//! logic; the only integration points with upstream code are one init hook call in
//! `init_after_frontend_ready` and (in a later phase) one outbound tee hook in
//! `send_frontend_message_to_js`.
//!
//! Configuration (checked at init, highest precedence first):
//!
//! 1. `localStorage` keys `graphite-remote-relay-url`, `graphite-remote-password`.
//! 2. A `window.__GRAPHITE_REMOTE__` object with `relayUrl` / `password` string properties
//!    (settable from the browser console, a userscript, or a future Vite define).
//!
//! If no relay URL is found the module is fully inert: no connection attempts, no behavior change.
//! The instance UUID is generated via `crypto.randomUUID()` and persisted per tab under the
//! `sessionStorage` key `graphite-remote-uuid`: each Graphite tab is an independent editor with its own
//! document state, so each tab is its own remote instance, and a reload keeps the tab's identity.
//! A duplicated tab inherits the original's `sessionStorage` and so its UUID; the relay then kicks the
//! older socket with the "replaced" close code, and the kicked tab stays disconnected until reloaded.
//!
//! This module carries the full relay client: connection lifecycle (hello, capped-exponential-backoff
//! reconnect); inbound command dispatch through the fallible `TryFrom` validation boundary, with
//! rejections broadcast back as `RemoteError` frames; the outbound allowlist tee (a compile-time-
//! exhaustive match — see `tee_frontend_message`); the flush-on-socket-receipt message pump that keeps
//! the editor responsive to remote traffic while the tab is hidden and `requestAnimationFrame` is
//! suspended; and the `resend_state` pseudo-command.

use editor::messages::prelude::{DocumentMessage, FrontendMessage, Message, NodeGraphMessage, PortfolioMessage};
use editor::utility_traits::{AsMessage, ToDiscriminant};
use std::cell::{Cell, RefCell};
use wasm_bindgen::prelude::*;
use web_sys::{CloseEvent, MessageEvent, WebSocket};

const PROTOCOL_VERSION: u64 = 1;
const STORAGE_KEY_RELAY_URL: &str = "graphite-remote-relay-url";
const STORAGE_KEY_PASSWORD: &str = "graphite-remote-password";
const STORAGE_KEY_UUID: &str = "graphite-remote-uuid";
/// Relay close code for a socket kicked by a newer connection claiming the same UUID (wire format, "Close codes").
const CLOSE_CODE_REPLACED: u16 = 4001;
const RECONNECT_BASE_MS: u32 = 500;
const RECONNECT_CAP_MS: u32 = 30_000;

#[derive(Clone)]
struct RemoteConfig {
	relay_url: String,
	password: String,
	uuid: String,
}

thread_local! {
	static SOCKET: RefCell<Option<WebSocket>> = const { RefCell::new(None) };
	// The closures attached to the current socket, kept alive until the socket is replaced
	static SOCKET_CALLBACKS: RefCell<Vec<JsValue>> = const { RefCell::new(Vec::new()) };
	static RECONNECT_TIMER_CLOSURE: RefCell<Option<Closure<dyn FnMut()>>> = const { RefCell::new(None) };
	static RECONNECT_ATTEMPT: Cell<u32> = const { Cell::new(0) };
	static CONFIG: RefCell<Option<RemoteConfig>> = const { RefCell::new(None) };
}

/// Called once from `init_after_frontend_ready`. Inert unless a relay URL is configured.
pub(crate) fn init_remote_communication() {
	let Some(config) = load_config() else {
		log::debug!("Remote control not configured (no relay URL); remote communication is disabled");
		return;
	};

	log::info!("Remote control enabled: relay {} as instance {}", config.relay_url, config.uuid);
	CONFIG.with_borrow_mut(|slot| *slot = Some(config.clone()));
	connect(&config);
}

fn load_config() -> Option<RemoteConfig> {
	let relay_url = config_value("relayUrl", STORAGE_KEY_RELAY_URL)?;
	let password = config_value("password", STORAGE_KEY_PASSWORD).unwrap_or_default();
	Some(RemoteConfig { relay_url, password, uuid: instance_uuid() })
}

/// A config value from `localStorage` (which wins) or the `window.__GRAPHITE_REMOTE__` object.
fn config_value(global_property: &str, storage_key: &str) -> Option<String> {
	let window = web_sys::window()?;

	if let Ok(Some(storage)) = window.local_storage()
		&& let Ok(Some(value)) = storage.get_item(storage_key)
		&& !value.is_empty()
	{
		return Some(value);
	}

	let global = js_sys::Reflect::get(&window, &JsValue::from_str("__GRAPHITE_REMOTE__")).ok()?;
	js_sys::Reflect::get(&global, &JsValue::from_str(global_property)).ok()?.as_string().filter(|value| !value.is_empty())
}

/// The persisted per-tab instance UUID, generated on first use in the tab.
fn instance_uuid() -> String {
	let storage = web_sys::window().and_then(|window| window.session_storage().ok().flatten());

	if let Some(storage) = &storage
		&& let Ok(Some(uuid)) = storage.get_item(STORAGE_KEY_UUID)
		&& !uuid.is_empty()
	{
		return uuid;
	}

	let uuid = web_sys::window()
		.and_then(|window| window.crypto().ok())
		.and_then(|crypto| crypto.random_uuid().into())
		.unwrap_or_else(|| format!("graphite-{:016x}", (js_sys::Math::random() * u64::MAX as f64) as u64));

	if let Some(storage) = &storage {
		let _ = storage.set_item(STORAGE_KEY_UUID, &uuid);
	}
	uuid
}

fn connect(config: &RemoteConfig) {
	let socket = match WebSocket::new(&config.relay_url) {
		Ok(socket) => socket,
		Err(error) => {
			log::warn!("Remote control: failed to open WebSocket to {}: {error:?}", config.relay_url);
			schedule_reconnect();
			return;
		}
	};

	let mut callbacks = Vec::new();

	let hello_config = config.clone();
	let open_socket = socket.clone();
	let on_open = Closure::<dyn FnMut()>::new(move || {
		RECONNECT_ATTEMPT.with(|attempt| attempt.set(0));
		let hello = serde_json::json!({
			"v": PROTOCOL_VERSION,
			"role": "graphite",
			"uuid": hello_config.uuid,
			"password": hello_config.password,
		});
		if let Err(error) = open_socket.send_with_str(&hello.to_string()) {
			log::warn!("Remote control: failed to send hello frame: {error:?}");
		} else {
			log::info!("Remote control: connected to relay and sent hello");
		}
	});
	socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));
	callbacks.push(on_open.into_js_value());

	let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
		let Some(text) = event.data().as_string() else {
			log::warn!("Remote control: dropping non-text frame from relay");
			return;
		};
		handle_inbound_frame(&text);
	});
	socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
	callbacks.push(on_message.into_js_value());

	let on_close = Closure::<dyn FnMut(CloseEvent)>::new(move |event: CloseEvent| {
		SOCKET.with_borrow_mut(|slot| *slot = None);
		// Reconnecting after being replaced would kick the newer holder of this UUID, which would reconnect and kick back
		if event.code() == CLOSE_CODE_REPLACED {
			log::warn!("Remote control: replaced by a newer connection with the same instance ID (likely a duplicated tab); not reconnecting until reload");
			return;
		}
		log::info!("Remote control: relay connection closed (code {}); scheduling reconnect", event.code());
		schedule_reconnect();
	});
	socket.set_onclose(Some(on_close.as_ref().unchecked_ref()));
	callbacks.push(on_close.into_js_value());

	let on_error = Closure::<dyn FnMut()>::new(move || {
		// The browser fires close after error, so reconnect scheduling happens in the close handler
		log::debug!("Remote control: WebSocket error event");
	});
	socket.set_onerror(Some(on_error.as_ref().unchecked_ref()));
	callbacks.push(on_error.into_js_value());

	SOCKET.with_borrow_mut(|slot| *slot = Some(socket));
	SOCKET_CALLBACKS.with_borrow_mut(|slot| *slot = callbacks);
}

fn schedule_reconnect() {
	let attempt = RECONNECT_ATTEMPT.with(|attempt| {
		let current = attempt.get();
		attempt.set(current.saturating_add(1));
		current
	});
	let delay_ms = RECONNECT_BASE_MS.saturating_mul(1 << attempt.min(6)).min(RECONNECT_CAP_MS);

	let closure = Closure::<dyn FnMut()>::new(move || {
		let config = CONFIG.with_borrow(|config| config.clone());
		if let Some(config) = config {
			connect(&config);
		}
	});

	let scheduled = web_sys::window().map(|window| window.set_timeout_with_callback_and_timeout_and_arguments_0(closure.as_ref().unchecked_ref(), delay_ms as i32));
	match scheduled {
		Some(Ok(_)) => RECONNECT_TIMER_CLOSURE.with_borrow_mut(|slot| *slot = Some(closure)),
		_ => log::warn!("Remote control: failed to schedule reconnect"),
	}
}

/// The payload shape delivered to a Graphite instance: the consumer envelope minus routing fields.
#[derive(serde::Deserialize)]
struct InboundPayload {
	v: u64,
	command: String,
	args: Option<serde_json::Value>,
	request_id: Option<String>,
}

fn handle_inbound_frame(text: &str) {
	// Parsed in two steps so a rejected payload can still echo its `request_id` in the error frame
	let value: serde_json::Value = match serde_json::from_str(text) {
		Ok(value) => value,
		Err(error) => {
			log::warn!("Remote control: rejecting non-JSON inbound frame: {error}");
			send_remote_error("bad_payload", &format!("payload is not valid JSON: {error}"), None);
			return;
		}
	};
	let request_id = value.get("request_id").and_then(|id| id.as_str()).map(str::to_string);

	let payload: InboundPayload = match serde_json::from_value(value) {
		Ok(payload) => payload,
		Err(error) => {
			log::warn!("Remote control: rejecting malformed inbound payload: {error}");
			send_remote_error("bad_payload", &format!("payload must be an object with integer `v` and string `command`: {error}"), request_id.as_deref());
			return;
		}
	};

	if payload.v != PROTOCOL_VERSION {
		log::warn!("Remote control: rejecting frame with unsupported protocol version {}", payload.v);
		send_remote_error("bad_version", &format!("unsupported protocol version {}", payload.v), request_id.as_deref());
		return;
	}

	if payload.command == "resend_state" {
		handle_resend_state(payload.request_id.as_deref());
	} else {
		dispatch_remote_command(payload);
	}

	// Flush-on-socket-receipt: rAF (which normally pumps the editor) is suspended in hidden tabs,
	// so every inbound frame drives a pump pass to keep remote traffic flowing regardless
	spawn_message_pump();
}

/// The `resend_state` pseudo-command: re-emit the observable baseline for late-joining consumers.
///
/// The document list re-emits unconditionally. The layer observables cannot: `UpdateDocumentLayerDetails`
/// and `UpdateDocumentLayerStructure` are built by editor-core paths that early-return unless the layers
/// panel is open (`update_layer_panel`, `DocumentStructureChanged`), and `UpdateActiveDocument` has no
/// re-emitting message at all. Making those unconditional would require editor-core changes (a new touch
/// point), deliberately not taken; consumers get the panel-gated best effort.
fn handle_resend_state(request_id: Option<&str>) {
	if crate::EDITOR_HAS_CRASHED.load(std::sync::atomic::Ordering::SeqCst) {
		send_remote_error("invalid_command", "editor has crashed; commands are no longer accepted", request_id);
		return;
	}

	crate::helpers::wrapper(|wrapper| {
		wrapper.dispatch(PortfolioMessage::UpdateOpenDocumentsList);
		// Details before structure, since structure entries only make sense against received details
		wrapper.dispatch(NodeGraphMessage::UpdateLayerPanel);
		wrapper.dispatch(DocumentMessage::DocumentStructureChanged);
	});
}

const PUMP_MAX_ROUNDS: usize = 32;

thread_local! {
	static PUMP_RUNNING: Cell<bool> = const { Cell::new(false) };
}

/// Pump the same internals the rAF tick drives — flush `MESSAGE_BUFFER`, release the dispatcher's
/// per-frame deferred messages, await node-graph evaluation — until no work remains, so remote commands
/// and their resulting updates flow while the tab is hidden. Never calls `animation_frame`: remote
/// pumping must not advance the animation clock.
fn spawn_message_pump() {
	if PUMP_RUNNING.with(|flag| flag.replace(true)) {
		return;
	}

	wasm_bindgen_futures::spawn_local(async {
		for round in 0..PUMP_MAX_ROUNDS {
			let had_buffered = crate::MESSAGE_BUFFER.with_borrow(|buffer| !buffer.is_empty());
			if had_buffered {
				let messages = crate::MESSAGE_BUFFER.take();
				crate::helpers::wrapper(|wrapper| wrapper.dispatch(Message::Batched { messages: messages.into() }));
			}

			let deferred = take_frame_deferred_messages();
			let had_deferred = !deferred.is_empty();
			// Each dispatched individually so it runs at the dispatcher's top level, as the rAF tick releases them.
			// Wrapped in a `Batched` message they would run in a nested queue, where the dispatcher defers them again.
			for message in deferred {
				crate::helpers::wrapper(|wrapper| wrapper.dispatch(message));
			}

			crate::helpers::poll_node_graph_evaluation().await;

			// Quiescent once a non-first round found no buffered or deferred work and the poll produced none.
			// (Never the first round: node-graph results can surface only on a subsequent poll.)
			if round > 0 && !had_buffered && !had_deferred && crate::MESSAGE_BUFFER.with_borrow(|buffer| buffer.is_empty()) {
				break;
			}
		}
		PUMP_RUNNING.with(|flag| flag.set(false));
	});
}

/// Take the dispatcher's per-frame deferred messages (`Dispatcher::frontend_update_messages`: layer structure,
/// overlays, rulers, scrollbars, properties refresh). The dispatcher releases these only on the rAF tick's
/// `IncrementFrameCounter`, which never fires in a hidden tab, so without this the layer structure produced by
/// remote commands and `resend_state` would be withheld from consumers until the tab is next shown. Draining
/// the field directly performs the same release without advancing the animation clock.
fn take_frame_deferred_messages() -> Vec<Message> {
	crate::EDITOR.with(|editor| {
		let mut guard = editor.try_lock();
		match guard.as_deref_mut() {
			Ok(Some(editor)) => std::mem::take(&mut editor.dispatcher.frontend_update_messages),
			_ => Vec::new(),
		}
	})
}

fn dispatch_remote_command(payload: InboundPayload) {
	if crate::EDITOR_HAS_CRASHED.load(std::sync::atomic::Ordering::SeqCst) {
		send_remote_error("invalid_command", "editor has crashed; commands are no longer accepted", payload.request_id.as_deref());
		return;
	}

	// Reassemble serde's externally-tagged enum encoding, `{"CommandName": {args}}`, from the wire payload's
	// separate `command`/`args` fields. Missing args decode as an empty object (commands without parameters).
	let args = payload.args.unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
	let mut tagged = serde_json::Map::new();
	tagged.insert(payload.command.clone(), args);

	let command: crate::EditorCommand = match serde_json::from_value(serde_json::Value::Object(tagged)) {
		Ok(command) => command,
		Err(error) => {
			log::warn!("Remote control: rejecting undeserializable command {}: {error}", payload.command);
			send_remote_error("bad_payload", &format!("unknown command or malformed args: {error}"), payload.request_id.as_deref());
			return;
		}
	};

	if let Err(error) = check_remote_command(&command) {
		log::warn!("Remote control: rejecting invalid command {}: {error}", payload.command);
		send_remote_error("invalid_command", &error, payload.request_id.as_deref());
		return;
	}

	// The conversion is the validation boundary: `TryFrom` rejects payloads that deserialized but violate
	// invariants (undefined modifier bits, wrong widget value shapes, out-of-range node ids)
	match editor::messages::prelude::Message::try_from(command) {
		Ok(message) => crate::helpers::wrapper(move |wrapper| wrapper.dispatch(message)),
		Err(error) => {
			log::warn!("Remote control: rejecting invalid command {}: {error}", payload.command);
			send_remote_error("invalid_command", &error, payload.request_id.as_deref());
		}
	}
}

/// Remote-only checks on argument values the editor core trusts because the JS UI can only produce valid ones,
/// but which panic (or, in release builds, hit undefined behavior) when a remote caller supplies them. Kept in this
/// fork-owned module rather than in upstream's command bodies so the checks add no upstream diff and leave the JS
/// path unchanged. Only stateless checks live here; stale-but-well-formed ids are a documented limit (see the host
/// repo's `design-spec/remote-control/remote-input-audit.md`). A new upstream command gets no check until added here.
fn check_remote_command(command: &crate::EditorCommand) -> Result<(), String> {
	use crate::EditorCommand;

	// `LayerNodeIdentifier` stores `id + 1` in a `NonZeroU64`: u64::MAX overflows, and 0 is the root parent
	let layer_id = |id: u64| if id == 0 || id == u64::MAX { Err("layer id out of range".to_string()) } else { Ok(()) };
	// The core adds per-layer offsets to the insert index without overflow checks (and `usize` is 32 bits on wasm)
	let insert_index = |index: Option<usize>| match index {
		Some(index) if index > i32::MAX as usize => Err("insert index out of range".to_string()),
		_ => Ok(()),
	};

	match command {
		EditorCommand::SelectLayer { id, .. } | EditorCommand::ClipLayer { id } | EditorCommand::SetLayerName { id, .. } => layer_id(*id),
		EditorCommand::MoveLayerInTree { insert_index: index, .. } | EditorCommand::DuplicateLayerInTree { insert_index: index, .. } => insert_index(*index),
		// `PanelType::from` panics on any other name
		EditorCommand::SetActivePanel { panel } => match panel.as_str() {
			"Welcome" | "Document" | "Layers" | "Properties" | "Data" => Ok(()),
			_ => Err(format!("unknown panel {panel:?}")),
		},
		// `DefinitionIdentifier::from_serialized` panics without one of these prefixes
		EditorCommand::CreateNode { node_type, .. } => match node_type.split_once(':') {
			Some(("PROTONODE" | "NETWORK", _)) => Ok(()),
			_ => Err("node type must start with PROTONODE: or NETWORK:".to_string()),
		},
		// The viewport handler asserts a positive scale
		EditorCommand::UpdateViewport { scale, .. } => {
			if scale.is_finite() && *scale > 0. {
				Ok(())
			} else {
				Err("viewport scale must be finite and positive".to_string())
			}
		}
		_ => Ok(()),
	}
}

/// Broadcast a wire-format `RemoteError` pseudo-message update frame back through the socket.
fn send_remote_error(code: &str, detail: &str, request_id: Option<&str>) {
	let Some(uuid) = CONFIG.with_borrow(|config| config.as_ref().map(|config| config.uuid.clone())) else {
		return;
	};

	let mut data = serde_json::Map::new();
	data.insert("error".to_string(), code.into());
	data.insert("detail".to_string(), detail.into());
	if let Some(request_id) = request_id {
		data.insert("request_id".to_string(), request_id.into());
	}
	let frame = serde_json::json!({
		"v": PROTOCOL_VERSION,
		"from_graphite_uuid": uuid,
		"message": "RemoteError",
		"data": data,
	});

	send_if_open(&frame.to_string(), "RemoteError frame");
}

/// Outbound tee, called from the single hook in `send_frontend_message_to_js` for every message on
/// the JS callback path (`UpdateImageData` returns early before the hook and never arrives — intended,
/// raster output is excluded from the wire). Forwards the v1 allowlist to the relay socket.
///
/// The allowlist is a compile-time-exhaustive match — no wildcard arm — so any upstream variant
/// addition, rename, or removal fails the build here and forces a deliberate allowlist decision
/// instead of a silent wire-protocol change. The allowlist itself is defined in the host repo's
/// `design-spec/remote-control/wire-format.md`; keep the two in lockstep.
pub(crate) fn tee_frontend_message(message: &FrontendMessage) {
	// Cheap early-out when remote control is inert, disconnected, or still connecting
	if !SOCKET.with_borrow(|socket| socket.as_ref().is_some_and(|socket| socket.ready_state() == WebSocket::OPEN)) {
		return;
	}

	#[rustfmt::skip]
	let allowlisted = match message {
		// ------ Allowlisted: observing document list, layer structure, selection, dialog, and tool state ------
		FrontendMessage::UpdateOpenDocumentsList { .. }
		| FrontendMessage::UpdateActiveDocument { .. }
		| FrontendMessage::UpdateDocumentLayerStructure { .. }
		| FrontendMessage::UpdateDocumentLayerDetails { .. }
		| FrontendMessage::UpdateNodeGraphSelection { .. }
		| FrontendMessage::DisplayDialog { .. }
		| FrontendMessage::DialogClose
		| FrontendMessage::DisplayDialogPanic { .. }
		| FrontendMessage::UpdateLayout { .. } => true,

		// ------ Dropped: bulk/continuous render output ------
		FrontendMessage::UpdateDocumentArtwork { .. }
		| FrontendMessage::UpdateImageData { .. }
		| FrontendMessage::UpdateNodeThumbnail { .. }
		| FrontendMessage::UpdateGraphFadeArtwork { .. }
		// ------ Dropped: per-input-event chatter ------
		| FrontendMessage::UpdateBox { .. }
		| FrontendMessage::UpdateDocumentRulers { .. }
		| FrontendMessage::UpdateDocumentScrollbars { .. }
		| FrontendMessage::UpdateMouseCursor { .. }
		| FrontendMessage::UpdateEyedropperSamplingState { .. }
		| FrontendMessage::UpdateWirePathInProgress { .. }
		| FrontendMessage::UpdateClickTargets { .. }
		| FrontendMessage::UpdateLayerWidths { .. }
		| FrontendMessage::UpdateImportReorderIndex { .. }
		| FrontendMessage::UpdateExportReorderIndex { .. }
		| FrontendMessage::UpdateGradientStopColorPickerPosition { .. }
		| FrontendMessage::ColorPickerColorChanged { .. }
		| FrontendMessage::ColorPickerStartHistoryTransaction
		| FrontendMessage::ColorPickerCommitHistoryTransaction
		// ------ Dropped: node-graph internals beyond selection ------
		| FrontendMessage::UpdateNodeGraphNodes { .. }
		| FrontendMessage::UpdateNodeGraphWires { .. }
		| FrontendMessage::ClearAllNodeGraphWires
		| FrontendMessage::UpdateVisibleNodes { .. }
		| FrontendMessage::UpdateNodeGraphErrorDiagnostic { .. }
		| FrontendMessage::UpdateNodeGraphTransform { .. }
		| FrontendMessage::UpdateImportsExports { .. }
		| FrontendMessage::UpdateInSelectedNetwork { .. }
		| FrontendMessage::UpdateGraphViewOverlay { .. }
		| FrontendMessage::UpdateContextMenuInformation { .. }
		// ------ Dropped: ByteBuf payloads and document-content leak paths ------
		| FrontendMessage::DisplayEditableTextbox { .. }
		| FrontendMessage::DisplayEditableTextboxUpdateFontData { .. }
		| FrontendMessage::TriggerSaveDocument { .. }
		| FrontendMessage::TriggerSaveFile { .. }
		// ------ Dropped: host-capability triggers answered by the human's browser ------
		| FrontendMessage::DisplayEditableTextboxTransform { .. }
		| FrontendMessage::DisplayRemoveEditableTextbox
		| FrontendMessage::SendUIMetadata { .. }
		| FrontendMessage::SendShortcutFullscreen { .. }
		| FrontendMessage::SendShortcutAltClick { .. }
		| FrontendMessage::SendShortcutShiftClick { .. }
		| FrontendMessage::TriggerAboutGraphiteLocalizedCommitDate { .. }
		| FrontendMessage::TriggerDisplayThirdPartyLicensesDialog
		| FrontendMessage::TriggerBrowse { .. }
		| FrontendMessage::TriggerExportImage { .. }
		| FrontendMessage::TriggerFetchAndOpenDocument { .. }
		| FrontendMessage::TriggerPersistenceReadState
		| FrontendMessage::TriggerPersistenceWriteState { .. }
		| FrontendMessage::TriggerOpenLaunchDocuments
		| FrontendMessage::TriggerLoadPreferences
		| FrontendMessage::TriggerSavePreferences { .. }
		| FrontendMessage::TriggerTextCommit
		| FrontendMessage::TriggerEditLayerNameInGraph { .. }
		| FrontendMessage::TriggerVisitLink { .. }
		| FrontendMessage::TriggerClipboardRead
		| FrontendMessage::TriggerClipboardWrite { .. }
		| FrontendMessage::TriggerSelectionRead { .. }
		| FrontendMessage::TriggerSelectionWrite { .. }
		// ------ Dropped: window/platform chrome ------
		| FrontendMessage::UpdateWorkspacePanelLayout { .. }
		| FrontendMessage::UpdatePlatform { .. }
		| FrontendMessage::UpdateMaximized { .. }
		| FrontendMessage::UpdateFullscreen { .. }
		| FrontendMessage::UpdateViewportHolePunch { .. }
		| FrontendMessage::UpdateUIScale { .. }
		| FrontendMessage::WindowPointerLockMove { .. }
		| FrontendMessage::WindowFullscreen => false,
	};

	if allowlisted {
		send_update_frame(message);
	}
}

/// Serialize an allowlisted message independently of the JS path (which uses `serde_wasm_bindgen`)
/// and send it as a wire-format Graphite update frame. `WebSocket::send` never synchronously
/// re-enters `dispatch()`.
fn send_update_frame(message: &FrontendMessage) {
	let Some(uuid) = CONFIG.with_borrow(|config| config.as_ref().map(|config| config.uuid.clone())) else {
		return;
	};

	// serde's externally-tagged encoding is `{"VariantName": {fields}}` for struct variants and
	// `"VariantName"` for unit variants; the wire frame carries the fields object alone as `data`
	let data = match serde_json::to_value(message) {
		Ok(serde_json::Value::Object(map)) => map.into_values().next().unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new())),
		Ok(_) => serde_json::Value::Object(serde_json::Map::new()),
		Err(error) => {
			log::warn!("Remote control: failed to serialize FrontendMessage for the socket: {error}");
			return;
		}
	};

	let frame = serde_json::json!({
		"v": PROTOCOL_VERSION,
		"from_graphite_uuid": uuid,
		"message": message.to_discriminant().local_name(),
		"data": data,
	});

	send_if_open(&frame.to_string(), "update frame");
}

/// Send a text frame if the socket is open. An absent, still-connecting, or closing socket drops the frame silently:
/// the socket exists from `WebSocket::new` onward, but `send` throws `InvalidStateError` until the open event fires.
fn send_if_open(text: &str, what: &str) {
	SOCKET.with_borrow(|socket| {
		if let Some(socket) = socket
			&& socket.ready_state() == WebSocket::OPEN
			&& let Err(error) = socket.send_with_str(text)
		{
			log::warn!("Remote control: failed to send {what}: {error:?}");
		}
	});
}
