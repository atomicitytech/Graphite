#[derive(clap::Parser)]
#[clap(name = "graphite", version)]
pub struct Cli {
	#[arg(help = "Files to open on startup")]
	pub files: Vec<std::path::PathBuf>,

	#[arg(long, action = clap::ArgAction::SetTrue, help = "Disable hardware accelerated UI rendering")]
	pub disable_ui_acceleration: bool,

	// Remote-control fork: relay address (ip:port or ws:// URL) and password for the native relay client (relay_client.rs)
	#[arg(long, value_name = "IP:PORT", help = "Connect to a remote-control relay")]
	pub tcp_relay: Option<String>,
	#[arg(long, value_name = "SECRET", help = "Remote-control relay password")]
	pub tcp_secret: Option<String>,
}
