//! Controller module for Shinka operator

pub mod discord;
pub mod events;
mod finalizers;
mod reconciler;
pub mod webhook;

pub use discord::{DiscordConfig, OperatorEventType, OptionalDiscordClient};
pub use events::{EventRecorder, OptionalEventRecorder};
pub use finalizers::{cleanup, FINALIZER};
pub use reconciler::{error_policy, reconcile, Context, ReconcileResult};
pub use webhook::{
    MigrationEvent, MigrationEventType, OptionalReleaseTrackerClient, ReleaseTrackerClient,
};
