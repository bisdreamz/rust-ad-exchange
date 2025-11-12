//! Houses pipeline related to the outgoing user sync process,
//! that is - when our process is first started and our
//! sync url is called. Then, we will drop demand or
//! outgoing sync URLs typically in our own iframe

pub mod pipeline;
pub mod context;
mod tasks;