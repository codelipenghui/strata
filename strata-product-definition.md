# Strata — Product Definition

**Working codename: Strata** (placeholder, pending naming review)

**Engineering companion:** [strata-tech-design.md](strata-tech-design.md) — architecture, protocols, failure design, and open technical questions. This document is intentionally short: it carries what is needed to evaluate and fund the product.

**What it is:** a Kafka-compatible streaming platform that stores log data in its own disaggregated storage layer instead of on broker disks — built for organizations that run their own infrastructure and cannot depend on a cloud object store.

## TL;DR

- **The gap.** Every modern fix for Kafka's storage problem — WarpStream, AutoMQ, Bufstream, Confluent Freight, Diskless Kafka (KIP-1150) — assumes an S3-class object store underneath. Self-managed datacenters, regulated and air-gapped environments, sovereign clouds, and edge deployments are excluded, yet they feel Kafka's storage pain most acutely.
- **The product.** Keep the Kafka protocol and ecosystem unchanged. Make brokers diskless and stateless. Store all log data in a purpose-built, quorum-replicated Strata file service on commodity disks. Coordinate with a compact, KRaft-based metadata plane.
- **The wins.** Storage scales by adding storage nodes, not brokers. Operations move metadata, never data. Cold data ages in place on cheap media — no tiering pipeline, because nothing ever needs to migrate. Quorum acks keep produce p99 flat.
- **The position.** Kafka compatibility + disaggregated storage + no object-store dependency. No product on the market offers all three.
- **The reach.** Sealed-data placement is pluggable — local media, object store, or hybrid — so one architecture serves both self-managed and cloud deployments.

## 1. The problem

Self-managed Kafka operators face four compounding problems:

- **Storage and compute scale together.** Retention grows faster than throughput, but adding storage means adding brokers — and a new broker does nothing until partitions are reassigned onto it. Draining one 50 TB broker at a production-safe throttle takes about three days, so clusters run permanently over-provisioned instead.
- **Cold data costs 3x, and the official fix taxes the hot path.** Kafka stores every byte at 3x replication for its full retention. Tiered storage (KIP-405) requires a remote object store — precisely what this market lacks (and the licensing trajectory of the common substitute, MinIO, made it a supply-chain risk) — and it makes the busiest nodes do the tiering: a 2 GB/s cluster adds a permanent 2 GB/s of leader disk re-reads plus 2 GB/s of leader NIC upload, and if uploads fall behind, the cluster stops accepting writes.
- **Tail latency is structurally unstable.** With `acks=all`, produce latency is gated by the slowest in-sync replica: one 200 ms GC pause on any follower puts 200 ms into produce p99, and ISR ejection (~30 s) is far too slow to protect against transient stalls.
- **Failure recovery and balancing move data wholesale.** Replacing a failed broker re-replicates everything it held; moving a hot partition drags its entire cold history with it.

## 2. What Strata is

Strata separates the Kafka protocol from log storage. Three independently scalable parts:

- **Diskless, stateless brokers** speak the full Kafka protocol — produce, fetch, consumer groups, transactions. They hold no durable state: restarts take seconds, any broker can lead any partition, and broker failure moves leadership (a metadata operation), never data.
- **A purpose-built Strata file service** on commodity disks. Actively written data is quorum-replicated (3 replicas, ack on 2) for low, flat write latency; sealed data is immutable and ages in place on cheap, dense media until retention deletes it. There is no tiering pipeline, because data never lives on brokers in the first place.
- **First-class file namespaces** let one Strata storage cluster serve multiple Kafka clusters or tenants while keeping paths, future ACLs, and quotas separate.
- **A compact metadata plane** built on Kafka's own KRaft — consulted at chunk boundaries and leadership changes, never per message.

Two operational facts that follow: a failed broker's 1,000 partitions re-lead in seconds with zero data movement; a failed 100 TB storage node is re-replicated by the whole pool in parallel — hours, not the days a Kafka broker replacement takes — and the exposure window *shrinks* as the cluster grows.

**Pluggable placement** is the strategic hinge: sealed data can target dense local media (self-managed), object storage (cloud), or both (hybrid) — one architecture for all three deployment modes (§5).

## 3. What you get

- **Elastic storage without an object store.** Retention becomes a configuration change backed by adding storage nodes — unavailable in any Kafka-compatible product today without an S3 dependency.
- **No partition outgrows a node.** Retention is bounded by aggregate pool capacity, not one broker's disks. (Capacity, not throughput: hot partitions still split for bandwidth.)
- **Zero-data-movement operations.** Hot partition on an overloaded broker: Kafka moves the partition — hours of throttled copying; Strata moves its leadership — seconds, zero bytes. Storage balance is passive: write-time placement plus retention converge on their own. There is no Cruise Control equivalent because there is nothing for it to do.
- **Cheap-media economics without tiering.** Cost is set by where data is placed at write time — dense HDD works, since the workload is sequential. Storage class is user configuration; Strata never migrates data between classes on its own. (Erasure coding remains a future option, not committed scope: 3x → ~1.4x.)
- **Flat produce p99.** Quorum acknowledgment structurally removes the slowest-replica coupling — measurable against Apache Kafka in any side-by-side benchmark. (Honest boundary: Redpanda's Raft also absorbs follower stalls; the case against Redpanda is storage disaggregation, not latency.)
- **Seconds-level failover, zero re-replication.** Truncation and divergence reconciliation are eliminated, not optimized.
- **Full ecosystem compatibility.** Producers, consumers, Connect, Streams, and tooling work unchanged. Migration is a protocol endpoint change, not an application rewrite — the decisive difference from Northguard-style clean-slate systems.

## 4. Competitive landscape

| | Kafka protocol | Storage scales independently | No object-store dependency | Cold-data cost | Produce p99 behavior | Balancing model | Status |
|---|---|---|---|---|---|---|---|
| **Apache Kafka** | native | no — coupled to brokers | yes | 3x replication | gated by slowest ISR replica | partition reassignment (data movement) | mature |
| **Kafka + tiered storage (KIP-405)** | native | partially (cold tier only) | **no** — requires remote store | object-store dependent | unchanged, plus tiering IO tax on leaders | unchanged | mature |
| **Redpanda** | yes | no — shared-nothing, coupled | yes (tiering optional) | 3x (local tier) | flat for follower stalls (Raft majority commit); leader stalls still gate | partition movement | mature |
| **WarpStream / Bufstream / Confluent Freight / Diskless (KIP-1150)** | yes | yes | **no — S3 is the architecture** | object-store pricing | hundreds of ms (direct, batched S3 writes) | stateless (S3 does it) | shipping |
| **AutoMQ** | yes | yes | **no** — S3 for log data (WAL on EBS/local disk) | object-store pricing | ms-level (block-storage WAL ack; S3 upload async) | metadata-only reassignment | shipping |
| **Pulsar + KoP (BookKeeper)** | via translation layer | yes | yes | 3x (EC not native to BK) | flat in principle (quorum writes); engine-internal jitter | passive (write-time placement) | mature |
| **LinkedIn Northguard** | **no — incompatible by design** | yes | yes | segment-level replication | n/a externally | segment-level | internal only |
| **Strata** | native | yes | yes | **3x on dense commodity media; EC a future exploration** | **flat (quorum writes)** | **leadership-only (brokers) + passive (storage)** | this proposal |

Four observations structure the field:

- The entire modern wave is S3-native: an excellent trade in public cloud, a non-answer in a datacenter.
- The systems offering disaggregated storage without S3 either abandon Kafka compatibility (Northguard) or carry a storage engine built for a different workload (BookKeeper under KoP — see tech design Appendix B for the full engineering comparison).
- LinkedIn's Northguard (2025) independently validates the architectural direction — segment-level replication, storage decoupled from serving — at the world's largest Kafka deployment. But it is internal, not open, and deliberately not Kafka-compatible.
- Nobody combines all three properties. That intersection is the product.

## 5. Strategy

**One product line, three deployment modes.** Strata is not a parallel bet against object-store streaming — it is the same architecture with a different placement target: dense local media (self-managed), object storage (cloud), hybrid (hot quorum tier local, cold tier in any object store). The quorum-replicated hot layer — the hard, differentiating engineering — is common to all three. This makes the on-prem market the entry point of a single product line, and gives cloud customers a repatriation and hybrid story that S3-native competitors structurally cannot match.

**A banked option, deliberately unbuilt.** The storage layer is Kafka-agnostic by discipline — in substance a general primitive for single-writer log workloads (WAL-as-a-service, lakehouse log layers). The category's graveyard (Pravega, LogDevice, DistributedLog: general stream-storage layers with no committed first tenant) dictates how to hold it: the layer stays Kafka-ignorant inside, the product speaks only Kafka outside, and any second tenant waits until the first has paid for production maturity.

**Why a product, not a KIP.** KIP-1150's design is shaped by object-store semantics — batched writes, hundreds-of-milliseconds acks — the structural opposite of a low-latency quorum path; there is no interface to plug into. And the surgery Strata performs is more than upstream could absorb on any realistic timeline (KIP-405, far smaller, took years). Fork-and-track keeps full compatibility while owning the storage engine.

**How it gets built.** Broker and metadata plane are derived from the Apache Kafka codebase — the fork-and-replace path AutoMQ has proven in production — inheriting the protocol surface, the coordinators, and KRaft, which converts the top compatibility risk from build risk into merge maintenance. The Strata file service is new code with no Kafka dependency. (Detail: tech design §2, §4, §10–§12.)

## 6. Delivery phasing

- **v0 — storage engine bootstrap (internal milestone, not a release).** The Strata file service, chunk protocol, and repair machinery are built and validated against a thin ZooKeeper-backed metadata service behind the same interfaces — decoupling the first implementation from the Kafka-fork timeline, so the riskiest new engineering is proven while the controller work lands in parallel. ZooKeeper is a development expedient with explicit retirement criteria, never a shipped configuration.
- **v1 — the structural wins.** Quorum-replicated Strata file service, diskless brokers, KRaft-based metadata plane, Kafka protocol core. Delivers what Kafka structurally cannot: independent storage scaling, seconds-level failover, flat produce p99, passive balancing. Cold data stays at 3x. Protocol compatibility is staffed as a first-class workstream.
- **v1.x — heterogeneous media.** Media-aware placement (latency-critical topics to NVMe, everything else to dense HDD) plus relocation and decommission tooling. Still no migration pipeline.
- **Future exploration — erasure coding (not committed).** Could cut cold-data overhead from 3x toward ~1.4x; the only forward dependency (a polymorphic chunk-layout descriptor) ships in v1. Kafka, KIP-405, and Northguard v1 all ship without EC — current scope concedes nothing.

## 7. Target customers and non-goals

**Primary:** organizations running Kafka on self-managed infrastructure with growing retention — financial services, telecom, industrial/automotive, public sector, regulated and air-gapped environments; typically 50+ brokers or 100+ TB retained, where storage economics and reassignment pain dominate operational cost.

**Secondary:** latency-sensitive Kafka users for whom direct-to-S3 architectures' hundreds-of-milliseconds produce latency is disqualifying, but who still want disaggregated storage. (AutoMQ serves this segment with a block-storage WAL in front of S3; against it the argument is the object-store dependency itself, not latency.)

**Non-goals (v1):** a general-purpose filesystem or object store; storage-layer tenants beyond Kafka (option preserved, not productized); multi-protocol support; geo-replication as a storage-layer primitive; an active data-balancing service; replacing the Kafka protocol with a proprietary client.

## 8. Risks

- **The adoption paradox is the top product risk.** The buyers who most need Strata — regulated, air-gapped, financial — are the most conservative storage adopters in existence, asked to trust v1 of a new storage engine with retention-mandated data. The structural mitigation is Kafka wire compatibility: migration is a side-by-side dual-run with gradual consumer cutover, and rollback is a connection-string change. The migration playbook deserves investment equal to any feature.
- **Kafka protocol compatibility is a long tail.** Transactions, group coordination, and per-version behaviors have historically cost more than the storage engine itself. Deriving the broker from the Kafka codebase converts most of this from build risk into merge-maintenance risk — a deliberate trade.
- **Broker NIC egress is one replication copy higher than a Kafka leader's** (three copies out versus two) — a hardware-model line item that must appear in sizing guides.
- **Passive balancing converges at retention speed** — slow precisely for long-retention customers; capacity-weighted placement carries the load meanwhile, and drain tooling is the escape hatch.
- **The category's history is explicit.** General stream-storage layers without a committed first tenant did not survive. Strata's day-one binding to Kafka is a constraint, not a preference.

Engineering risks and open design questions (durable-offset staleness, metadata scaling levers, fork baseline, security pass) are tracked in [strata-tech-design.md](strata-tech-design.md) §14 and §17.
