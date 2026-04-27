# Shinka

> **★★★ CSE / Knowable Construction.** This repo operates under **Constructive Substrate Engineering** — canonical specification at [`pleme-io/theory/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md`](https://github.com/pleme-io/theory/blob/main/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md). The Compounding Directive (operational rules: solve once, load-bearing fixes only, idiom-first, models stay current, direction beats velocity) is in the org-level pleme-io/CLAUDE.md ★★★ section. Read both before non-trivial changes.

GitOps-native database migration operator for Kubernetes. Shinka watches for deployment image changes and automatically runs database migrations using Kubernetes Jobs, with CloudNativePG health checks, checksum reconciliation, leader election, and full observability. Built in Rust with [kube-rs](https://github.com/kube-rs/kube).
