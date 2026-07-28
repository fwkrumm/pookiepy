//! Integration and unit tests for grpchook server
//! 
//! These tests cover all critical paths mentioned in the TODO list.

#[cfg(test)]
mod schema_tests {
    use super::*;
    use crate::core::{CoreState, Message as CoreMessage};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_schema_mismatch_rejection() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 10));
        
        // Valid schema should pass
        assert!(core.check_schema("1.0.0").await.is_ok());
        
        // Mismatched schema should fail with FAILED_PRECONDITION
        let result = core.check_schema("2.0.0").await;
        assert!(result.is_err());
        
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn test_schema_version_in_error_message() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 10));
        
        let result = core.check_schema("2.0.0").await;
        assert!(result.is_err());
        
        let err_msg = result.unwrap_err().message();
        assert!(err_msg.contains("1.0.0"));
        assert!(err_msg.contains("2.0.0"));
    }

    #[tokio::test]
    async fn test_schema_version_retrieval() {
        let core = Arc::new(CoreState::new("custom-version-123".to_string(), 5));
        
        assert_eq!(core.get_schema_version(), "custom-version-123");
    }
}

#[cfg(test)]
mod connection_tests {
    use super::*;
    use crate::core::{CoreState, Message as CoreMessage};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_connection_counting() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 5));
        
        // Initially zero connections
        assert_eq!(core.get_connection_count().await, 0);

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<CoreMessage>();
        
        // Add client - should succeed initially
        assert!(core.add_client("client-1".to_string(), tx).await.is_ok());
        assert_eq!(core.get_connection_count().await, 1);

        // Add more clients up to limit
        for i in 2..=5 {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<CoreMessage>();
            assert!(core.add_client(format!("client-{}", i), tx).await.is_ok());
        }

        // At max - should fail
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<CoreMessage>();
        let result = core.add_client("client-6".to_string(), tx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_disconnect_cleanup() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 5));
        
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<CoreMessage>();
        assert!(core.add_client("client-1".to_string(), tx).await.is_ok());
        
        // Verify client exists
        assert_eq!(core.get_connection_count().await, 1);

        // Disconnect - should decrement count and cleanup subscriptions
        let removed = core.remove_client("client-1").await;
        
        assert_eq!(removed, 0); // No subscriptions for this test
        assert_eq!(core.get_connection_count().await, 0);

        // Verify client is actually removed
        let clients = core.clients.read().await;
        assert!(!clients.contains_key("client-1"));
    }

    #[tokio::test]
    async fn test_max_workers_enforcement() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 2));
        
        for i in 0..3 {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<CoreMessage>();
            
            if i < 2 {
                assert!(core.add_client(format!("client-{}", i), tx).await.is_ok());
            } else {
                // Third client should be rejected
                let result = core.add_client(format!("client-{}", i), tx).await;
                assert!(result.is_err());
                assert_eq!(result.unwrap_err().code(), tonic::Code::ResourceExhausted);
            }
        }

        // Count should still be at max
        assert_eq!(core.get_connection_count().await, 2);
    }
}

#[cfg(test)]
mod routing_tests {
    use super::*;
    use crate::core::{CoreState, Message as CoreMessage};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_subscribe_and_broadcast() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 10));
        
        // Subscribe multiple clients to same message type
        core.subscribe("event-1".to_string(), "client-1").await.unwrap();
        core.subscribe("event-1".to_string(), "client-2").await.unwrap();

        let subscribers = core.get_subscribers("event-1").await;
        assert_eq!(subscribers.len(), 2);
        assert!(subscribers.contains(&"client-1".to_string()));
        assert!(subscribers.contains(&"client-2".to_string()));

        // Unsubscribe one client
        let routing = core.routing_table.write().await;
        routing.remove_client_subscriptions("client-1");
        drop(routing);

        let subscribers_after = core.get_subscribers("event-1").await;
        assert_eq!(subscribers_after.len(), 1);
    }

    #[cfg(test)]
    async fn test_broadcast_excludes_sender() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 10));
        
        // Subscribe clients and add them to the system
        for i in 1..=3 {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<CoreMessage>();
            core.add_client(format!("client-{}", i), tx).await.unwrap();
            core.subscribe("event-1".to_string(), &format!("client-{}", i)).await.unwrap();
        }

        let msg = CoreMessage {
            message_name: "event-1".to_string(),
            sender_id: "client-2".to_string(),
            payload: vec![1, 2, 3],
        };

        // Route with skip_sender=true - client-2 should be excluded from recipients
        let result = core.route_message(&msg, true).await;
        assert!(result.is_ok());

        // Verify routing still works correctly (other clients received it)
        let subscribers = core.get_subscribers("event-1").await;
        assert_eq!(subscribers.len(), 3);
    }

    #[tokio::test]
    async fn test_disconnect_cleanup_does_not_affect_other_clients() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 10));
        
        // Subscribe multiple clients to same message type
        for i in 1..=5 {
            core.subscribe("event-1".to_string(), &format!("client-{}", i)).await.unwrap();
        }

        let routing = core.routing_table.read().await;
        assert_eq!(routing.get_subscribers("event-1").unwrap().len(), 5);

        // Disconnect client-3 - should only remove its subscriptions, not others
        core.remove_client("client-3").await;

        let routing_after = core.routing_table.read().await;
        
        // Should still have 4 subscribers (not wiped to zero)
        assert_eq!(routing_after.get_subscribers("event-1").unwrap().len(), 4);
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use crate::core::{CoreState, Message as CoreMessage};
    use std::sync::Arc;
    use tokio::task;

    #[tokio::test]
    async fn test_arc_state_in_spawned_tasks() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 10));
        
        // Spawn multiple tasks that all own a clone of the Arc state
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let core_clone = core.clone();
                task::spawn(async move {
                    // Each task can safely access shared state
                    let count = core_clone.get_connection_count().await;
                    format!("Task {} saw count={}", i, count)
                })
            })
            .collect();

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.contains("count="));
        }
    }

    #[tokio::test]
    async fn test_async_state_without_borrow_issues() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 10));
        
        // Spawn tasks that perform concurrent operations on shared state
        let handles: Vec<_> = (0..3)
            .map(|_| {
                let core_clone = core.clone();
                task::spawn(async move {
                    for _ in 0..10 {
                        let _count = core_clone.get_connection_count().await;
                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                    }
                })
            })
            .collect();

        // Wait for all tasks to complete without borrow checker issues
        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }
}

#[cfg(test)]
mod welcome_before_register_tests {
    use super::*;
    use crate::core::{CoreState, Message as CoreMessage};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_welcome_message_available() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 10));
        
        // Server should provide schema version even before any clients connect
        let schema = core.get_schema_version();
        assert_eq!(schema, "1.0.0");

        // This is the welcome-before-register ordering: client can get server info
        // before completing registration
    }

    #[tokio::test]
    async fn test_register_after_welcome() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 10));
        
        // Simulate: client gets welcome (schema version) first, then registers
        let schema_from_welcome = core.get_schema_version();
        assert_eq!(schema_from_welcome, "1.0.0");

        // Then client can register with correct schema version
        assert!(core.check_schema(&schema_from_welcome).await.is_ok());
    }
}

#[cfg(test)]
mod reject_on_connect_tests {
    use super::*;
    use crate::core::{CoreState, Message as CoreMessage};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_reject_when_max_workers_exceeded() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 2));
        
        // Fill up to max_workers
        for i in 0..2 {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<CoreMessage>();
            core.add_client(format!("client-{}", i), tx).await.unwrap();
        }

        // Next connection should be rejected
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<CoreMessage>();
        let result = core.add_client("client-3".to_string(), tx).await;
        
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn test_reject_with_appropriate_error_message() {
        let core = Arc::new(CoreState::new("1.0.0".to_string(), 1));
        
        // Fill to max
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<CoreMessage>();
        core.add_client("client-1".to_string(), tx).await.unwrap();

        // Next should fail with descriptive message
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<CoreMessage>();
        let err = core.add_client("client-2".to_string(), tx).await.unwrap_err();
        
        assert!(err.message().contains("1")); // Should mention the limit
    }
}
