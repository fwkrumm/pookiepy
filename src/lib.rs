//! grpchook-server - Rust implementation of the Python gRPC server
//! 
//! This library provides the core functionality for the grpchook messaging system,
//! including schema validation, client connection management, and message routing.

pub mod core;
pub use crate::core::{CoreState, Message as CoreMessage, Client, RoutingTable};

// Re-export gRPC service implementation  
pub mod grpc;
pub use crate::grpc::{GrpchookService, GrpchookServiceServer};

// Threading utilities for async task management
pub mod threading;
pub use crate::threading::{WorkerPool, TaskManager, TaskState, TaskStatus};

// Re-export generated types from proto compilation (available after build.rs runs)
// These are automatically included by tonic-build at compile time
pub mod grpchook_client_proto {
    pub use crate::grpchook_server::*;
}

#[cfg(test)]
mod tests;
