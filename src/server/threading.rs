//! Threading and concurrency primitives for the server.
//!
//! This module provides the `NotificationQueue` type, which is used to queue messages
//! for delivery to a specific client. It also contains logic for managing client connections.

use tokio::sync::mpsc;
use dashmap::DashMap;

/// A channel for sending notifications to a specific client.
/// Using a bounded sender to match the Python `queue.Queue` behavior with `max_queue_elements`.
pub type NotificationQueue = mpsc::Sender<super::core::Message>;

// The following would be used in the Python version's `DataRegister` class.
// In Rust, we use `DashMap` for thread-safe access to the data register.
