//! Remote-control fork: native relay client for the desktop build.
//!
//! Connects the desktop editor to the relay server as a Graphite instance, per the wire format in the host
//! repo's `design-spec/remote-control/wire-format.md` and the placement in `desktop-integration.md`. It is the
//! desktop counterpart of the web build's `remote_communication.rs`; frame building, validation, and the
//! outbound allowlist live in the shared `graphite_wasm_wrapper::remote_protocol`.
//!
//! Configuration comes from the `--tcp-relay <ip:port>` and `--tcp-secret <secret>` command-line flags, handed over
//! by `configure` from `lib.rs`. Without `--tcp-relay` the client is inert; without `--tcp-secret` the password is
//! empty. The instance UUID is per launch: a fresh v4 UUID, logged at info level, nothing persisted.
//!
//! The connection runs on a detached thread with a current-thread tokio runtime: connect, hello, inbound frames
//! through `remote_protocol::handle_inbound` (rejections replied on the socket, accepted messages scheduled onto
//! the event loop as `FromWeb`), outbound frames from a channel, and capped-exponential-backoff reconnect slept
//! outside the runtime. Frames are dropped, not queued, while disconnected. No reconnect after the relay's
//! "replaced" close code, nor once shutdown has begun.

//!
//! Integration is one hook at the dispatch funnel (`on_responses`, called from `App::dispatch_desktop_wrapper_message`)
//! plus `shutdown` on the exit path. The hook starts the client lazily on the first `OpenLaunchDocuments`, i.e. once
//! the editor has processed `PortfolioMessage::Init`, so no remote command can be dispatched before `Init`.

use crate::event::{AppEvent, AppEventScheduler};
use crate::wrapper::messages::{DesktopFrontendMessage, DesktopWrapperMessage};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures::{SinkExt, StreamExt};
use graphite_wasm_wrapper::remote_protocol::{self, Inbound};
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// The flag values from `configure`: `(relay, secret)` as given on the command line.
static FLAGS: OnceLock<(Option<String>, Option<String>)> = OnceLock::new();

/// The relay client, set once: `Some` after the lazy start when configured, `None` when unconfigured, failed to start,
/// or shut down before starting.
static CLIENT: OnceLock<Option<RelayClient>> = OnceLock::new();

// ===================
// Dispatch-funnel hook
// ===================

/// Called with every batch of editor results before the app handles them. Starts the client on the first
/// `OpenLaunchDocuments`, then tees allowlisted outbound `ToWeb` messages to the relay while the socket is open.
/// `responses` is mutable so a later export diversion can claim messages from it.
pub(crate) fn on_responses(scheduler: &AppEventScheduler, responses: &mut Vec<DesktopFrontendMessage>) {
	if CLIENT.get().is_none() && signals_init(responses) {
		CLIENT.get_or_init(|| start(scheduler.clone()));
	}
	if let Some(Some(client)) = CLIENT.get() {
		tee(client, responses);
	}
}

/// Stop the relay client, if running, and prevent a later start. Called once the event loop has exited.
pub(crate) fn shutdown() {
	if let Some(client) = CLIENT.get_or_init(|| None) {
		client.shutdown();
	}
}

/// `OpenLaunchDocuments` comes from `FrontendMessage::TriggerOpenLaunchDocuments`, emitted once by `PortfolioMessage::Init`.
fn signals_init(responses: &[DesktopFrontendMessage]) -> bool {
	responses.iter().any(|message| matches!(message, DesktopFrontendMessage::OpenLaunchDocuments))
}

/// Encode and send the allowlisted messages of every `ToWeb` batch. Skips encoding entirely while disconnected.
fn tee(client: &RelayClient, responses: &[DesktopFrontendMessage]) {
	if !client.is_open() {
		return;
	}
	for message in responses {
		if let DesktopFrontendMessage::ToWeb(messages) = message {
			remote_protocol::encode_updates(messages, client.uuid()).into_iter().for_each(|frame| client.send(frame));
		}
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RelayConfig {
	pub(crate) relay_url: String,
	pub(crate) password: String,
	pub(crate) uuid: String,
}

/// Handle to the running relay client thread. Dropping it, or calling `shutdown`, stops the client.
pub(crate) struct RelayClient {
	uuid: String,
	open: Arc<AtomicBool>,
	shutdown: Arc<AtomicBool>,
	outbound: UnboundedSender<String>,
}

impl RelayClient {
	/// The instance UUID to stamp on outbound frames (`remote_protocol::encode_updates`).
	pub(crate) fn uuid(&self) -> &str {
		&self.uuid
	}

	/// Whether the socket is open and has sent hello. Callers skip encoding outbound frames while this is false.
	pub(crate) fn is_open(&self) -> bool {
		self.open.load(Ordering::SeqCst)
	}

	/// Queue a serialized frame for the relay, or drop it if the socket is not open.
	pub(crate) fn send(&self, frame: String) {
		if self.is_open() {
			let _ = self.outbound.unbounded_send(frame);
		}
	}

	/// Stop the client without blocking: closes an open connection and prevents any further reconnect.
	pub(crate) fn shutdown(&self) {
		self.shutdown.store(true, Ordering::SeqCst);
		self.outbound.close_channel();
	}
}

/// Start the relay client if remote control is configured, scheduling accepted remote messages onto the event
/// loop through `scheduler`. Returns `None` (and starts nothing) when unconfigured or the thread fails to spawn.
pub(crate) fn start(scheduler: AppEventScheduler) -> Option<RelayClient> {
	let (relay, secret) = FLAGS.get().cloned().unwrap_or_default();
	let Some(config) = config_from_flags(relay, secret, generate_uuid()) else {
		tracing::debug!("Remote control not configured (no --tcp-relay); relay client is disabled");
		return None;
	};
	tracing::info!("Remote control: instance id {}", config.uuid);
	tracing::info!("Remote control enabled: relay {}", config.relay_url);

	// The same path, and FIFO order, as UI commands
	let dispatch = move |message| scheduler.schedule(AppEvent::DesktopWrapperMessage(message));
	spawn(config, dispatch).map(|(client, _)| client)
}

fn spawn(config: RelayConfig, dispatch: impl Fn(DesktopWrapperMessage) + Send + 'static) -> Option<(RelayClient, std::thread::JoinHandle<()>)> {
	let open = Arc::new(AtomicBool::new(false));
	let shutdown = Arc::new(AtomicBool::new(false));
	let (outbound, receiver) = unbounded();
	let client = RelayClient {
		uuid: config.uuid.clone(),
		open: open.clone(),
		shutdown: shutdown.clone(),
		outbound,
	};

	// Detached, never joined: a join would hang exit while a connect to an unreachable relay times out
	let spawned = std::thread::Builder::new()
		.name("relay-client".to_string())
		.spawn(move || run(config, dispatch, receiver, open, shutdown));
	match spawned {
		Ok(handle) => Some((client, handle)),
		Err(error) => {
			tracing::error!("Remote control: failed to spawn the relay client thread: {error}");
			None
		}
	}
}

// ===================
// Configuration
// ===================

/// Record the `--tcp-relay` and `--tcp-secret` flag values. Called once from `lib.rs` after argument parsing.
pub(crate) fn configure(relay: Option<String>, secret: Option<String>) {
	let _ = FLAGS.set((relay, secret));
}

/// The relay flags to pass to a restarted process, so a restart keeps remote control as launched.
pub(crate) fn restart_args() -> Vec<String> {
	FLAGS.get().map(|(relay, secret)| flag_args(relay, secret)).unwrap_or_default()
}

fn flag_args(relay: &Option<String>, secret: &Option<String>) -> Vec<String> {
	let flag = |name: &str, value: &Option<String>| value.iter().flat_map(|value| [name.to_string(), value.clone()]).collect::<Vec<_>>();
	[flag("--tcp-relay", relay), flag("--tcp-secret", secret)].concat()
}

/// Build the config from the flag values. `None` when `relay` is absent or empty. A bare `ip:port` becomes
/// `ws://ip:port`; a value already starting with `ws://` is used as-is. An absent secret is an empty password.
fn config_from_flags(relay: Option<String>, secret: Option<String>, uuid: String) -> Option<RelayConfig> {
	let relay = relay.map(|relay| relay.trim().to_string()).filter(|relay| !relay.is_empty())?;
	let relay_url = if relay.starts_with("ws://") { relay } else { format!("ws://{relay}") };
	Some(RelayConfig { relay_url, password: secret.unwrap_or_default(), uuid })
}

/// A random version 4 UUID in canonical hyphenated form.
fn generate_uuid() -> String {
	let mut bytes = rand::random::<u128>().to_be_bytes();
	bytes[6] = (bytes[6] & 0x0f) | 0x40;
	bytes[8] = (bytes[8] & 0x3f) | 0x80;
	let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
	format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}

// ===================
// Connection
// ===================

enum SessionEnd {
	/// Connection failed or closed: reconnect after backoff.
	Reconnect,
	/// Closed with the "replaced" code: a newer connection holds this UUID.
	Replaced,
	/// The outbound channel closed: the client was shut down or dropped.
	Shutdown,
}

fn run(config: RelayConfig, dispatch: impl Fn(DesktopWrapperMessage), mut receiver: UnboundedReceiver<String>, open: Arc<AtomicBool>, shutdown: Arc<AtomicBool>) {
	let runtime = match tokio::runtime::Builder::new_current_thread().enable_io().build() {
		Ok(runtime) => runtime,
		Err(error) => {
			tracing::error!("Remote control: failed to build the relay client runtime: {error}");
			return;
		}
	};

	let mut attempt = 0;
	loop {
		if shutdown.load(Ordering::SeqCst) {
			return;
		}

		let end = runtime.block_on(session(&config, &dispatch, &mut receiver, &open, &mut attempt));
		open.store(false, Ordering::SeqCst);
		match end {
			SessionEnd::Reconnect => {}
			SessionEnd::Replaced => {
				tracing::warn!("Remote control: replaced by a newer connection with the same instance ID; not reconnecting until restart");
				return;
			}
			SessionEnd::Shutdown => return,
		}

		let delay = remote_protocol::reconnect_delay(attempt);
		attempt = attempt.saturating_add(1);
		tracing::info!("Remote control: reconnecting in {} ms", delay.as_millis());
		std::thread::sleep(delay);

		if shutdown.load(Ordering::SeqCst) {
			return;
		}
	}
}

/// One connection: connect, hello, then relay frames both ways until the socket or the outbound channel closes.
async fn session(config: &RelayConfig, dispatch: &impl Fn(DesktopWrapperMessage), receiver: &mut UnboundedReceiver<String>, open: &AtomicBool, attempt: &mut u32) -> SessionEnd {
	let mut socket = match tokio_tungstenite::connect_async(config.relay_url.as_str()).await {
		Ok((socket, _)) => socket,
		Err(error) => {
			tracing::warn!("Remote control: failed to connect to {}: {error}", config.relay_url);
			return SessionEnd::Reconnect;
		}
	};

	if let Err(error) = socket.send(WsMessage::Text(remote_protocol::hello(&config.uuid, &config.password))).await {
		tracing::warn!("Remote control: failed to send hello frame: {error}");
		return SessionEnd::Reconnect;
	}
	*attempt = 0;

	// Discard anything that reached the channel while disconnected, then accept outbound frames
	loop {
		match receiver.try_next() {
			Ok(Some(_)) => continue,
			Ok(None) => return SessionEnd::Shutdown,
			Err(_) => break,
		}
	}
	open.store(true, Ordering::SeqCst);
	tracing::info!("Remote control: connected to relay and sent hello");

	loop {
		tokio::select! {
			inbound = socket.next() => match inbound {
				Some(Ok(WsMessage::Text(text))) => match remote_protocol::handle_inbound(&text, &config.uuid) {
					Inbound::Reply(frame) => {
						if let Err(error) = socket.send(WsMessage::Text(frame)).await {
							tracing::warn!("Remote control: failed to send RemoteError frame: {error}");
						}
					}
					// Each dispatched separately, in order, as distinct top-level messages
					Inbound::Dispatch { messages, .. } => messages.into_iter().for_each(|message| dispatch(DesktopWrapperMessage::FromWeb(Box::new(message)))),
				},
				Some(Ok(WsMessage::Close(frame))) => {
					let code = frame.as_ref().map(|frame| u16::from(frame.code));
					// Reconnecting after being replaced would kick the newer holder of this UUID, which would reconnect and kick back
					if code == Some(remote_protocol::CLOSE_CODE_REPLACED) {
						return SessionEnd::Replaced;
					}
					tracing::info!("Remote control: relay connection closed (code {code:?})");
					return SessionEnd::Reconnect;
				}
				Some(Ok(WsMessage::Binary(_))) => tracing::warn!("Remote control: dropping non-text frame from relay"),
				// Pings are answered by tungstenite itself
				Some(Ok(_)) => {}
				Some(Err(error)) => {
					tracing::info!("Remote control: relay connection lost: {error}");
					return SessionEnd::Reconnect;
				}
				None => return SessionEnd::Reconnect,
			},
			outbound = receiver.next() => match outbound {
				Some(frame) => {
					if let Err(error) = socket.send(WsMessage::Text(frame)).await {
						tracing::warn!("Remote control: failed to send update frame: {error}");
						return SessionEnd::Reconnect;
					}
				}
				None => {
					let _ = socket.close(None).await;
					return SessionEnd::Shutdown;
				}
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::net::{TcpListener, TcpStream};
	use std::sync::mpsc;
	use std::time::{Duration, Instant};
	use tokio_tungstenite::tungstenite::protocol::CloseFrame;
	use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
	use tokio_tungstenite::tungstenite::{WebSocket, accept};

	const TIMEOUT: Duration = Duration::from_secs(5);

	// ===================
	// Hook
	// ===================

	/// A client whose socket counts as open or closed, without a thread; the receiver collects what `send` queues.
	fn detached_client(open: bool) -> (RelayClient, UnboundedReceiver<String>) {
		let (outbound, receiver) = unbounded();
		let client = RelayClient {
			uuid: "test-instance".into(),
			open: Arc::new(AtomicBool::new(open)),
			shutdown: Arc::new(AtomicBool::new(false)),
			outbound,
		};
		(client, receiver)
	}

	/// A `ToWeb` batch from serialized `FrontendMessage`s (the desktop crate cannot name the type).
	fn to_web(json: &str) -> DesktopFrontendMessage {
		DesktopFrontendMessage::ToWeb(serde_json::from_str(json).unwrap())
	}

	fn drain(receiver: &mut UnboundedReceiver<String>) -> Vec<serde_json::Value> {
		std::iter::from_fn(|| receiver.try_next().ok().flatten()).map(|frame| serde_json::from_str(&frame).unwrap()).collect()
	}

	#[test]
	fn init_is_signalled_only_by_open_launch_documents() {
		assert!(!signals_init(&[]));
		assert!(!signals_init(&[to_web("[]"), DesktopFrontendMessage::WindowClose]));
		assert!(signals_init(&[to_web("[]"), DesktopFrontendMessage::OpenLaunchDocuments]));
	}

	#[test]
	fn tee_sends_only_allowlisted_messages_from_every_batch() {
		let (client, mut receiver) = detached_client(true);
		let responses = [
			to_web(r#"["DialogClose", "ColorPickerStartHistoryTransaction"]"#),
			DesktopFrontendMessage::OpenLaunchDocuments,
			to_web(r#"["DialogClose"]"#),
		];
		tee(&client, &responses);

		let frames = drain(&mut receiver);
		assert_eq!(frames.len(), 2);
		for frame in frames {
			assert_eq!(frame["message"], "DialogClose");
			assert_eq!(frame["from_graphite_uuid"], "test-instance");
		}
	}

	#[test]
	fn tee_sends_nothing_while_disconnected() {
		let (client, mut receiver) = detached_client(false);
		tee(&client, &[to_web(r#"["DialogClose"]"#)]);
		assert!(drain(&mut receiver).is_empty());
	}

	fn flags(relay: Option<&str>, secret: Option<&str>) -> Option<RelayConfig> {
		config_from_flags(relay.map(str::to_string), secret.map(str::to_string), "id".into())
	}

	#[test]
	fn absent_or_empty_relay_flag_is_inert() {
		assert_eq!(flags(None, None), None);
		assert_eq!(flags(None, Some("secret")), None);
		assert_eq!(flags(Some(""), Some("secret")), None);
		assert_eq!(flags(Some("  "), None), None);
	}

	#[test]
	fn bare_address_gets_ws_scheme() {
		let config = flags(Some("127.0.0.1:41232"), Some("secret")).unwrap();
		assert_eq!(config, RelayConfig { relay_url: "ws://127.0.0.1:41232".into(), password: "secret".into(), uuid: "id".into() });
	}

	#[test]
	fn ws_url_is_accepted_as_is() {
		assert_eq!(flags(Some("ws://relay.local:7343"), None).unwrap().relay_url, "ws://relay.local:7343");
	}

	#[test]
	fn omitted_secret_is_empty_password() {
		assert_eq!(flags(Some("127.0.0.1:41232"), None).unwrap().password, "");
	}

	#[test]
	fn restart_args_repeat_the_given_flags() {
		assert!(flag_args(&None, &None).is_empty());
		assert_eq!(flag_args(&Some("127.0.0.1:41232".into()), &None), ["--tcp-relay", "127.0.0.1:41232"]);
		assert_eq!(flag_args(&Some("127.0.0.1:41232".into()), &Some("secret".into())), ["--tcp-relay", "127.0.0.1:41232", "--tcp-secret", "secret"]);
	}

	#[test]
	fn generated_uuids_are_distinct_v4() {
		let (a, b) = (generate_uuid(), generate_uuid());
		assert_ne!(a, b);
		for uuid in [a, b] {
			let groups: Vec<_> = uuid.split('-').map(str::len).collect();
			assert_eq!(groups, [8, 4, 4, 4, 12]);
			assert!(matches!(&uuid[19..20], "8" | "9" | "a" | "b"));
		}
	}

	// ===================
	// Loopback relay
	// ===================

	struct Harness {
		listener: TcpListener,
		client: RelayClient,
		thread: std::thread::JoinHandle<()>,
		dispatched: mpsc::Receiver<bool>,
		config: RelayConfig,
	}

	fn harness() -> Harness {
		let listener = TcpListener::bind("127.0.0.1:0").unwrap();
		listener.set_nonblocking(true).unwrap();
		let config = RelayConfig {
			relay_url: format!("ws://{}", listener.local_addr().unwrap()),
			password: "secret".into(),
			uuid: "test-instance".into(),
		};
		let (dispatched_sender, dispatched) = mpsc::channel();
		let (client, thread) = spawn(config.clone(), move |message| dispatched_sender.send(matches!(message, DesktopWrapperMessage::FromWeb(_))).unwrap()).unwrap();
		Harness { listener, client, thread, dispatched, config }
	}

	/// Accept the next client connection within `timeout`, completing the WebSocket handshake.
	fn accept_within(listener: &TcpListener, timeout: Duration) -> Option<WebSocket<TcpStream>> {
		let deadline = Instant::now() + timeout;
		loop {
			match listener.accept() {
				Ok((stream, _)) => {
					stream.set_nonblocking(false).unwrap();
					stream.set_read_timeout(Some(TIMEOUT)).unwrap();
					return Some(accept(stream).unwrap());
				}
				Err(error) if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
				Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return None,
				Err(error) => panic!("accept failed: {error}"),
			}
		}
	}

	fn read_text(socket: &mut WebSocket<TcpStream>) -> String {
		loop {
			match socket.read().unwrap() {
				WsMessage::Text(text) => return text,
				WsMessage::Ping(_) | WsMessage::Pong(_) => continue,
				other => panic!("expected a text frame, got {other:?}"),
			}
		}
	}

	fn wait_until(condition: impl Fn() -> bool) {
		let deadline = Instant::now() + TIMEOUT;
		while !condition() {
			assert!(Instant::now() < deadline, "timed out waiting for condition");
			std::thread::sleep(Duration::from_millis(10));
		}
	}

	fn close_with(socket: &mut WebSocket<TcpStream>, code: u16) {
		socket.close(Some(CloseFrame { code: CloseCode::from(code), reason: "".into() })).unwrap();
		// Drive the close handshake until the client's reply or the connection drop
		while socket.read().is_ok() {}
	}

	#[test]
	fn sends_hello_and_opens() {
		let h = harness();
		let mut socket = accept_within(&h.listener, TIMEOUT).unwrap();
		assert_eq!(read_text(&mut socket), remote_protocol::hello(&h.config.uuid, &h.config.password));
		wait_until(|| h.client.is_open());
	}

	#[test]
	fn rejection_is_replied_and_command_is_dispatched() {
		let h = harness();
		let mut socket = accept_within(&h.listener, TIMEOUT).unwrap();
		read_text(&mut socket);

		socket.send(WsMessage::Text("not json".into())).unwrap();
		let reply: serde_json::Value = serde_json::from_str(&read_text(&mut socket)).unwrap();
		assert_eq!(reply["message"], "RemoteError");
		assert_eq!(reply["from_graphite_uuid"], "test-instance");
		assert_eq!(reply["data"]["error"], "bad_payload");

		socket.send(WsMessage::Text(r#"{"v":1,"command":"resend_state"}"#.into())).unwrap();
		// Expanded to three rebuild messages, each scheduled as its own `FromWeb`
		for _ in 0..3 {
			assert!(h.dispatched.recv_timeout(TIMEOUT).unwrap());
		}
	}

	#[test]
	fn frames_are_dropped_while_disconnected() {
		let h = harness();
		// Before the connection opens: refused by `send`, and discarded by the thread if it reached the channel anyway
		assert!(!h.client.is_open());
		h.client.send("dropped by send".into());
		h.client.outbound.unbounded_send("discarded by thread".into()).unwrap();

		let mut socket = accept_within(&h.listener, TIMEOUT).unwrap();
		read_text(&mut socket);
		wait_until(|| h.client.is_open());
		h.client.send("delivered".into());
		assert_eq!(read_text(&mut socket), "delivered");
	}

	#[test]
	fn reconnects_after_ordinary_close() {
		let h = harness();
		let mut socket = accept_within(&h.listener, TIMEOUT).unwrap();
		read_text(&mut socket);
		close_with(&mut socket, 1000);

		let mut socket = accept_within(&h.listener, TIMEOUT).expect("client should reconnect");
		assert_eq!(read_text(&mut socket), remote_protocol::hello(&h.config.uuid, &h.config.password));
	}

	#[test]
	fn does_not_reconnect_after_replaced() {
		let h = harness();
		let mut socket = accept_within(&h.listener, TIMEOUT).unwrap();
		read_text(&mut socket);
		close_with(&mut socket, remote_protocol::CLOSE_CODE_REPLACED);

		// Well past the first backoff delay
		let window = remote_protocol::reconnect_delay(0) * 3;
		assert!(accept_within(&h.listener, window).is_none(), "client reconnected after being replaced");
		assert!(h.thread.is_finished());
		assert!(!h.client.is_open());
	}

	#[test]
	fn shutdown_closes_connection_and_stops_thread() {
		let h = harness();
		let mut socket = accept_within(&h.listener, TIMEOUT).unwrap();
		read_text(&mut socket);
		wait_until(|| h.client.is_open());

		h.client.shutdown();
		assert!(matches!(socket.read(), Ok(WsMessage::Close(_))));
		wait_until(|| h.thread.is_finished());
		assert!(accept_within(&h.listener, remote_protocol::reconnect_delay(0) * 2).is_none());
	}
}
