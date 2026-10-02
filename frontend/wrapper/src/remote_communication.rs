//! Fork-owned relay client for the remote-control feature (web build only).
//!
//! Connects the running editor to the relay server as a WebSocket client, per the wire format in
//! the host repo's `design-spec/remote-control/wire-format.md`. This module and `remote_protocol` own
//! all remote-control logic; the only integration points with upstream code are one init hook call in
//! `init_after_frontend_ready` and one outbound tee hook in `send_frontend_message_to_js`.
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
//! This module carries the browser side of the relay client: connection lifecycle (hello, capped-exponential-
//! backoff reconnect); inbound frames handed to `remote_protocol` for validation, with rejections broadcast back
//! as `RemoteError` frames and accepted commands dispatched unless the editor has crashed; the outbound tee into
//! `remote_protocol`'s allowlist (see `tee_frontend_message`); and the flush-on-socket-receipt message pump that
//! keeps the editor responsive to remote traffic while the tab is hidden and `requestAnimationFrame` is suspended.
//! Frame building, validation, and the allowlist live in the platform-neutral `remote_protocol`, shared with the
//! desktop client.

use crate::remote_protocol::{self, Inbound};
use editor::messages::prelude::{FrontendMessage, Message};
use std::cell::{Cell, RefCell};
use wasm_bindgen::prelude::*;
use web_sys::{CloseEvent, MessageEvent, WebSocket};

const STORAGE_KEY_RELAY_URL: &str = "graphite-remote-relay-url";
const STORAGE_KEY_PASSWORD: &str = "graphite-remote-password";
const STORAGE_KEY_UUID: &str = "graphite-remote-uuid";

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
		let hello = remote_protocol::hello(&hello_config.uuid, &hello_config.password);
		if let Err(error) = open_socket.send_with_str(&hello) {
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
		if event.code() == remote_protocol::CLOSE_CODE_REPLACED {
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
	let delay_ms = remote_protocol::reconnect_delay(attempt).as_millis() as i32;

	let closure = Closure::<dyn FnMut()>::new(move || {
		let config = CONFIG.with_borrow(|config| config.clone());
		if let Some(config) = config {
			connect(&config);
		}
	});

	let scheduled = web_sys::window().map(|window| window.set_timeout_with_callback_and_timeout_and_arguments_0(closure.as_ref().unchecked_ref(), delay_ms));
	match scheduled {
		Some(Ok(_)) => RECONNECT_TIMER_CLOSURE.with_borrow_mut(|slot| *slot = Some(closure)),
		_ => log::warn!("Remote control: failed to schedule reconnect"),
	}
}

fn handle_inbound_frame(text: &str) {
	let Some(uuid) = CONFIG.with_borrow(|config| config.as_ref().map(|config| config.uuid.clone())) else {
		return;
	};

	match remote_protocol::handle_inbound(text, &uuid) {
		Inbound::Reply(frame) => send_if_open(&frame, "RemoteError frame"),
		Inbound::Dispatch { request_id, .. } if crate::EDITOR_HAS_CRASHED.load(std::sync::atomic::Ordering::SeqCst) => {
			let frame = remote_protocol::remote_error("invalid_command", "editor has crashed; commands are no longer accepted", request_id.as_deref(), &uuid);
			send_if_open(&frame, "RemoteError frame");
		}
		Inbound::Dispatch { messages, .. } => {
			// Each dispatched separately, in order, as distinct top-level messages
			for message in messages {
				crate::helpers::wrapper(move |wrapper| wrapper.dispatch(message));
			}
		}
	}

	// Flush-on-socket-receipt: rAF (which normally pumps the editor) is suspended in hidden tabs,
	// so every inbound frame drives a pump pass to keep remote traffic flowing regardless
	spawn_message_pump();
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

/// Outbound tee, called from the single hook in `send_frontend_message_to_js` for every message on
/// the JS callback path (`UpdateImageData` returns early before the hook and never arrives — intended,
/// raster output is excluded from the wire). Forwards the messages `remote_protocol`'s v1 allowlist
/// passes to the relay socket. `WebSocket::send` never synchronously re-enters `dispatch()`.
pub(crate) fn tee_frontend_message(message: &FrontendMessage) {
	// Cheap early-out when remote control is inert, disconnected, or still connecting
	if !SOCKET.with_borrow(|socket| socket.as_ref().is_some_and(|socket| socket.ready_state() == WebSocket::OPEN)) {
		return;
	}
	let Some(uuid) = CONFIG.with_borrow(|config| config.as_ref().map(|config| config.uuid.clone())) else {
		return;
	};

	for frame in remote_protocol::encode_updates(std::slice::from_ref(message), &uuid) {
		send_if_open(&frame, "update frame");
	}
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
