use std::sync::OnceLock;
use rtb::server::Server;

#[derive(Default)]
pub struct StartupContext {
    pub server: OnceLock<Server>
}