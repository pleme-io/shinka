//! Database module for CNPG cluster health checks

mod cnpg;

pub use cnpg::{check_cluster_health, CnpgClusterHealth};
