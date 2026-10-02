//! Fork-owned, platform-neutral protocol logic for the remote-control feature.
//!
//! Implements the wire format in the host repo's `design-spec/remote-control/wire-format.md`: hello and
//! update frame building, inbound command validation, and the outbound allowlist. The interface is text in,
//! text out, with no platform types, so the web client (`remote_communication.rs`) and the desktop relay
//! client share one implementation and cannot drift. Transport, configuration, identity, and the
//! crashed-editor guard stay with each client.

use editor::messages::prelude::{DocumentMessage, FrontendMessage, Message, NodeGraphMessage, PortfolioMessage};
use editor::utility_traits::{AsMessage, ToDiscriminant};
use std::time::Duration;

pub const PROTOCOL_VERSION: u64 = 1;
/// Relay close code for a socket kicked by a newer connection claiming the same UUID (wire format, "Close codes").
pub const CLOSE_CODE_REPLACED: u16 = 4001;
const RECONNECT_BASE_MS: u64 = 500;
const RECONNECT_CAP_MS: u64 = 30_000;

/// The outcome of one inbound relay frame.
#[derive(Debug)]
pub enum Inbound {
	/// A validated command, or `resend_state` expanded to its rebuild messages. Each message is dispatched
	/// individually, in order. `request_id` is echoed if the client itself must still reject the command
	/// (the web client does so once the editor has crashed).
	Dispatch { messages: Vec<Message>, request_id: Option<String> },
	/// A serialized `RemoteError` update frame to send back on the socket.
	Reply(String),
}

/// The hello frame a Graphite instance sends on connect.
pub fn hello(uuid: &str, password: &str) -> String {
	serde_json::json!({
		"v": PROTOCOL_VERSION,
		"role": "graphite",
		"uuid": uuid,
		"password": password,
	})
	.to_string()
}

/// Capped exponential backoff before reconnect attempt number `attempt` (counting from 0).
pub fn reconnect_delay(attempt: u32) -> Duration {
	Duration::from_millis(RECONNECT_BASE_MS.saturating_mul(1 << attempt.min(6)).min(RECONNECT_CAP_MS))
}

/// The payload shape delivered to a Graphite instance: the consumer envelope minus routing fields.
#[derive(serde::Deserialize)]
struct InboundPayload {
	v: u64,
	command: String,
	args: Option<serde_json::Value>,
	request_id: Option<String>,
}

/// Validate one inbound text frame. Rejections become `RemoteError` frames stamped with `uuid`.
pub fn handle_inbound(text: &str, uuid: &str) -> Inbound {
	// Parsed in two steps so a rejected payload can still echo its `request_id` in the error frame
	let value: serde_json::Value = match serde_json::from_str(text) {
		Ok(value) => value,
		Err(error) => {
			log::warn!("Remote control: rejecting non-JSON inbound frame: {error}");
			return Inbound::Reply(remote_error("bad_payload", &format!("payload is not valid JSON: {error}"), None, uuid));
		}
	};
	let request_id = value.get("request_id").and_then(|id| id.as_str()).map(str::to_string);

	let payload: InboundPayload = match serde_json::from_value(value) {
		Ok(payload) => payload,
		Err(error) => {
			log::warn!("Remote control: rejecting malformed inbound payload: {error}");
			let detail = format!("payload must be an object with integer `v` and string `command`: {error}");
			return Inbound::Reply(remote_error("bad_payload", &detail, request_id.as_deref(), uuid));
		}
	};

	if payload.v != PROTOCOL_VERSION {
		log::warn!("Remote control: rejecting frame with unsupported protocol version {}", payload.v);
		return Inbound::Reply(remote_error("bad_version", &format!("unsupported protocol version {}", payload.v), request_id.as_deref(), uuid));
	}

	let request_id = payload.request_id.clone();
	let messages = if payload.command == "resend_state" {
		resend_state_messages()
	} else {
		match validate_command(payload) {
			Ok(message) => vec![message],
			Err((code, detail)) => return Inbound::Reply(remote_error(code, &detail, request_id.as_deref(), uuid)),
		}
	};
	Inbound::Dispatch { messages, request_id }
}

/// The `resend_state` pseudo-command: re-emit the observable baseline for late-joining consumers.
///
/// The document list re-emits unconditionally. The layer observables cannot: `UpdateDocumentLayerDetails`
/// and `UpdateDocumentLayerStructure` are built by editor-core paths that early-return unless the layers
/// panel is open (`update_layer_panel`, `DocumentStructureChanged`), and `UpdateActiveDocument` has no
/// re-emitting message at all. Making those unconditional would require editor-core changes (a new touch
/// point), deliberately not taken; consumers get the panel-gated best effort.
fn resend_state_messages() -> Vec<Message> {
	vec![
		PortfolioMessage::UpdateOpenDocumentsList.into(),
		// Details before structure, since structure entries only make sense against received details
		NodeGraphMessage::UpdateLayerPanel.into(),
		DocumentMessage::DocumentStructureChanged.into(),
	]
}

/// JSON → `EditorCommand` → `check_remote_command` → `Message::try_from`, returning the error code and detail on rejection.
fn validate_command(payload: InboundPayload) -> Result<Message, (&'static str, String)> {
	// Reassemble serde's externally-tagged enum encoding, `{"CommandName": {args}}`, from the wire payload's
	// separate `command`/`args` fields. Missing args decode as an empty object (commands without parameters).
	let args = payload.args.unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
	let mut tagged = serde_json::Map::new();
	tagged.insert(payload.command.clone(), args);

	let command: crate::EditorCommand = match serde_json::from_value(serde_json::Value::Object(tagged)) {
		Ok(command) => command,
		Err(error) => {
			log::warn!("Remote control: rejecting undeserializable command {}: {error}", payload.command);
			return Err(("bad_payload", format!("unknown command or malformed args: {error}")));
		}
	};

	if let Err(error) = check_remote_command(&command) {
		log::warn!("Remote control: rejecting invalid command {}: {error}", payload.command);
		return Err(("invalid_command", error));
	}

	// The conversion is the validation boundary: `TryFrom` rejects payloads that deserialized but violate
	// invariants (undefined modifier bits, wrong widget value shapes, out-of-range node ids)
	Message::try_from(command).map_err(|error| {
		log::warn!("Remote control: rejecting invalid command {}: {error}", payload.command);
		("invalid_command", error)
	})
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

/// A wire-format `RemoteError` pseudo-message update frame, broadcast from instance `uuid`.
pub fn remote_error(code: &str, detail: &str, request_id: Option<&str>, uuid: &str) -> String {
	let mut data = serde_json::Map::new();
	data.insert("error".to_string(), code.into());
	data.insert("detail".to_string(), detail.into());
	if let Some(request_id) = request_id {
		data.insert("request_id".to_string(), request_id.into());
	}
	serde_json::json!({
		"v": PROTOCOL_VERSION,
		"from_graphite_uuid": uuid,
		"message": "RemoteError",
		"data": data,
	})
	.to_string()
}

/// Apply the v1 allowlist to `messages` and serialize each allowlisted one as an update frame from instance `uuid`.
pub fn encode_updates(messages: &[FrontendMessage], uuid: &str) -> Vec<String> {
	messages.iter().filter(|message| is_allowlisted(message)).filter_map(|message| update_frame(message, uuid)).collect()
}

/// The allowlist is a compile-time-exhaustive match — no wildcard arm — so any upstream variant
/// addition, rename, or removal fails the build here and forces a deliberate allowlist decision
/// instead of a silent wire-protocol change. The allowlist itself is defined in the host repo's
/// `design-spec/remote-control/wire-format.md`; keep the two in lockstep.
fn is_allowlisted(message: &FrontendMessage) -> bool {
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

		// ------ Dropped: native-only window/platform chrome (a `cfg` cannot apply to one alternative of an or-pattern) ------
		#[cfg(not(target_family = "wasm"))]
		FrontendMessage::UpdateViewportPhysicalBounds { .. }
		| FrontendMessage::RenderOverlays { .. }
		| FrontendMessage::WindowPointerLock
		| FrontendMessage::WindowUpdateDirectInput { .. }
		| FrontendMessage::WindowClose
		| FrontendMessage::WindowMinimize
		| FrontendMessage::WindowMaximize
		| FrontendMessage::WindowDrag
		| FrontendMessage::WindowHide
		| FrontendMessage::WindowFocus
		| FrontendMessage::WindowHideOthers
		| FrontendMessage::WindowShowAll
		| FrontendMessage::WindowRestart => false,
	};
	allowlisted
}

/// Serialize a message independently of the JS path (which uses `serde_wasm_bindgen`) as a wire-format
/// Graphite update frame.
fn update_frame(message: &FrontendMessage, uuid: &str) -> Option<String> {
	// serde's externally-tagged encoding is `{"VariantName": {fields}}` for struct variants and
	// `"VariantName"` for unit variants; the wire frame carries the fields object alone as `data`
	let data = match serde_json::to_value(message) {
		Ok(serde_json::Value::Object(map)) => map.into_values().next().unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new())),
		Ok(_) => serde_json::Value::Object(serde_json::Map::new()),
		Err(error) => {
			log::warn!("Remote control: failed to serialize FrontendMessage for the socket: {error}");
			return None;
		}
	};

	let frame = serde_json::json!({
		"v": PROTOCOL_VERSION,
		"from_graphite_uuid": uuid,
		"message": message.to_discriminant().local_name(),
		"data": data,
	});
	Some(frame.to_string())
}

#[cfg(test)]
mod tests {
	use super::*;

	const UUID: &str = "test-uuid";

	fn json(text: &str) -> serde_json::Value {
		serde_json::from_str(text).expect("frame is valid JSON")
	}

	/// The error code, detail, and echoed request id of a `Reply`, asserting the frame's envelope on the way.
	fn reply(inbound: Inbound) -> (String, String, Option<String>) {
		let Inbound::Reply(text) = inbound else { panic!("expected Reply, got {inbound:?}") };
		let frame = json(&text);
		assert_eq!(frame["v"], 1);
		assert_eq!(frame["from_graphite_uuid"], UUID);
		assert_eq!(frame["message"], "RemoteError");
		let data = &frame["data"];
		(
			data["error"].as_str().unwrap().to_string(),
			data["detail"].as_str().unwrap().to_string(),
			data["request_id"].as_str().map(str::to_string),
		)
	}

	fn reply_code(text: &str) -> String {
		reply(handle_inbound(text, UUID)).0
	}

	fn dispatch(text: &str) -> (Vec<Message>, Option<String>) {
		match handle_inbound(text, UUID) {
			Inbound::Dispatch { messages, request_id } => (messages, request_id),
			Inbound::Reply(text) => panic!("expected Dispatch, got Reply {text}"),
		}
	}

	// ------ Framing ------

	#[test]
	fn hello_frame_shape() {
		assert_eq!(json(&hello("abc", "secret")), serde_json::json!({ "v": 1, "role": "graphite", "uuid": "abc", "password": "secret" }));
	}

	#[test]
	fn remote_error_frame_shape() {
		let frame = json(&remote_error("bad_payload", "oops", Some("r1"), UUID));
		assert_eq!(
			frame,
			serde_json::json!({ "v": 1, "from_graphite_uuid": UUID, "message": "RemoteError", "data": { "error": "bad_payload", "detail": "oops", "request_id": "r1" } })
		);
	}

	#[test]
	fn remote_error_omits_absent_request_id() {
		let frame = json(&remote_error("bad_version", "oops", None, UUID));
		assert!(frame["data"].as_object().unwrap().get("request_id").is_none());
	}

	#[test]
	fn update_frame_for_struct_variant_carries_fields_as_data() {
		let frames = encode_updates(&[FrontendMessage::DisplayDialogPanic { panic_info: "info".into() }], UUID);
		assert_eq!(frames.len(), 1);
		let frame = json(&frames[0]);
		assert_eq!(frame["v"], 1);
		assert_eq!(frame["from_graphite_uuid"], UUID);
		assert_eq!(frame["message"], "DisplayDialogPanic");
		assert_eq!(
			frame["data"],
			serde_json::to_value(FrontendMessage::DisplayDialogPanic { panic_info: "info".into() }).unwrap()["DisplayDialogPanic"]
		);
	}

	#[test]
	fn update_frame_for_unit_variant_has_empty_data() {
		let frames = encode_updates(&[FrontendMessage::DialogClose], UUID);
		assert_eq!(frames.len(), 1);
		let frame = json(&frames[0]);
		assert_eq!(frame["message"], "DialogClose");
		assert_eq!(frame["data"], serde_json::json!({}));
	}

	#[test]
	fn reconnect_delay_backs_off_exponentially_to_cap() {
		let delays: Vec<u64> = (0..10).map(|attempt| reconnect_delay(attempt).as_millis() as u64).collect();
		assert_eq!(delays, [500, 1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000, 30000]);
		assert_eq!(reconnect_delay(u32::MAX), Duration::from_millis(30_000));
	}

	#[test]
	fn close_code_replaced_matches_wire_format() {
		assert_eq!(CLOSE_CODE_REPLACED, 4001);
	}

	// ------ Validation ------

	#[test]
	fn valid_command_dispatches_one_message() {
		let (messages, request_id) = dispatch(r#"{"v":1,"command":"SetActivePanel","args":{"panel":"Layers"},"request_id":"r1"}"#);
		assert_eq!(messages.len(), 1);
		assert_eq!(request_id.as_deref(), Some("r1"));
	}

	#[test]
	fn command_without_args_dispatches() {
		let (messages, request_id) = dispatch(r#"{"v":1,"command":"ResendAllLayouts"}"#);
		assert_eq!(messages.len(), 1);
		assert_eq!(request_id, None);
	}

	#[test]
	fn resend_state_expands_to_rebuild_messages() {
		let (messages, request_id) = dispatch(r#"{"v":1,"command":"resend_state","request_id":"r2"}"#);
		assert_eq!(messages.len(), 3);
		assert_eq!(request_id.as_deref(), Some("r2"));
	}

	#[test]
	fn non_json_is_bad_payload_without_request_id() {
		for text in ["", "not json", r#"{"v":1,"command":"#, "\u{0}"] {
			let (code, _, request_id) = reply(handle_inbound(text, UUID));
			assert_eq!(code, "bad_payload", "input {text:?}");
			assert_eq!(request_id, None);
		}
	}

	#[test]
	fn wrong_shape_is_bad_payload() {
		for text in [
			"[]",
			"42",
			r#""str""#,
			"null",
			r#"{}"#,
			r#"{"v":1}"#,
			r#"{"command":"SetActivePanel"}"#,
			r#"{"v":"1","command":"SetActivePanel"}"#,
			r#"{"v":-1,"command":"X"}"#,
			r#"{"v":1,"command":7}"#,
		] {
			assert_eq!(reply_code(text), "bad_payload", "input {text:?}");
		}
	}

	#[test]
	fn malformed_payload_echoes_readable_request_id() {
		let (code, _, request_id) = reply(handle_inbound(r#"{"v":1,"request_id":"r3"}"#, UUID));
		assert_eq!(code, "bad_payload");
		assert_eq!(request_id.as_deref(), Some("r3"));
	}

	#[test]
	fn unsupported_version_is_bad_version() {
		let (code, detail, request_id) = reply(handle_inbound(r#"{"v":2,"command":"resend_state","request_id":"r4"}"#, UUID));
		assert_eq!(code, "bad_version");
		assert!(detail.contains('2'));
		assert_eq!(request_id.as_deref(), Some("r4"));
	}

	#[test]
	fn unknown_command_or_bad_args_is_bad_payload() {
		assert_eq!(reply_code(r#"{"v":1,"command":"NoSuchCommand"}"#), "bad_payload");
		assert_eq!(reply_code(r#"{"v":1,"command":"SetActivePanel","args":{"panel":3}}"#), "bad_payload");
		assert_eq!(reply_code(r#"{"v":1,"command":"SetActivePanel","args":[]}"#), "bad_payload");
		assert_eq!(reply_code(r#"{"v":1,"command":"SelectLayer","args":{"id":-1,"ctrl":false,"shift":false}}"#), "bad_payload");
	}

	#[test]
	fn remote_checks_reject_panicking_arguments() {
		let rejected = [
			r#"{"v":1,"command":"SelectLayer","args":{"id":18446744073709551615,"ctrl":false,"shift":false}}"#,
			r#"{"v":1,"command":"SelectLayer","args":{"id":0,"ctrl":false,"shift":false}}"#,
			r#"{"v":1,"command":"SetActivePanel","args":{"panel":"Nope"}}"#,
		];
		for text in rejected {
			let (code, _, request_id) = reply(handle_inbound(&text.replace("}}", r#"},"request_id":"r5"}"#), UUID));
			assert_eq!(code, "invalid_command", "input {text}");
			assert_eq!(request_id.as_deref(), Some("r5"));
		}
	}

	#[test]
	fn try_from_rejects_invariant_violations() {
		let (code, detail, _) = reply(handle_inbound(r#"{"v":1,"command":"OnKeyDown","args":{"name":"KeyA","modifiers":255,"key_repeat":false}}"#, UUID));
		assert_eq!(code, "invalid_command");
		assert_eq!(detail, "undefined modifier key bits");
	}

	// ------ Allowlist ------

	#[test]
	fn allowlisted_messages_are_encoded() {
		let allowlisted = [
			FrontendMessage::UpdateOpenDocumentsList { open_documents: Vec::new() },
			FrontendMessage::DialogClose,
			FrontendMessage::DisplayDialogPanic { panic_info: "info".into() },
		];
		assert_eq!(encode_updates(&allowlisted, UUID).len(), allowlisted.len());
	}

	#[test]
	fn dropped_messages_are_not_encoded() {
		let dropped = [
			FrontendMessage::ClearAllNodeGraphWires,
			FrontendMessage::TriggerTextCommit,
			FrontendMessage::TriggerClipboardRead,
			FrontendMessage::WindowFullscreen,
			FrontendMessage::UpdateUIScale { scale: 1. },
			#[cfg(not(target_family = "wasm"))]
			FrontendMessage::WindowClose,
		];
		assert!(encode_updates(&dropped, UUID).is_empty());
	}

	#[test]
	fn mixed_batch_keeps_allowlisted_in_order() {
		let batch = [
			FrontendMessage::TriggerTextCommit,
			FrontendMessage::DialogClose,
			FrontendMessage::WindowFullscreen,
			FrontendMessage::DisplayDialogPanic { panic_info: "x".into() },
		];
		let names: Vec<String> = encode_updates(&batch, UUID).iter().map(|frame| json(frame)["message"].as_str().unwrap().to_string()).collect();
		assert_eq!(names, ["DialogClose", "DisplayDialogPanic"]);
	}
}
