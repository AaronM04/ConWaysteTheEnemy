use std::net::SocketAddr;
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Endpoint(pub SocketAddr);
