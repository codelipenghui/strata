-------------------------- MODULE MetadataIdempotency --------------------------
(***************************************************************************)
(* Models operation-id idempotency for ONE namespace across leader            *)
(* failover and lost-ack retries (sections 8, 9, 13, 15 of                    *)
(* strata-metadata-scaling-design.md, and the review's idempotency finding).  *)
(*                                                                         *)
(* The design (section 8) CLAIMS every mutating client request carries an     *)
(* operation id, and that the leader records applied operation ids in durable *)
(* state (log/snapshot) so retries are idempotent across failover. Section 9  *)
(* lists an "idempotency table watermark"; section 13's ACTIVE barrier lists  *)
(* "restore retained idempotency state"; section 15 makes idempotency         *)
(* retention a log-truncation lower bound.                                    *)
(*                                                                         *)
(* THE FINDING: this bounded operation-id dedup table DOES NOT EXIST in the   *)
(* records / in-memory state / snapshot codec. Idempotency today is           *)
(* STRUCTURAL only: re-applying an identical record is a no-op, a conflicting *)
(* record is rejected, and the only carried op id is the create-op-id         *)
(* anchored to the fileId on FileCreated/ChunkCreated/ChunkAborted. Seal,     *)
(* delete, and replica records carry NO op id.                                *)
(*                                                                         *)
(* What this model isolates: an operation with an AT-MOST-ONCE side effect    *)
(* that is NOT idempotent-by-structure -- it ALLOCATES A FRESH RESOURCE       *)
(* (think: a request that mints a new id / appends a fresh-resource record     *)
(* whose retry would mint a SECOND one). The harm is a DUPLICATE side effect. *)
(*                                                                         *)
(* The lifecycle modeled, per client op (opId):                               *)
(*   1. the client sends the op;                                              *)
(*   2. the leader APPLIES it (allocates the resource, ACKs);                 *)
(*   3. the ACK is LOST, so the client RETRIES the same opId;                 *)
(*   4. a FAILOVER drops the leader's in-memory NON-durable dedup state and a *)
(*      successor takes over (durable state survives; in-memory does not);    *)
(*   5. the retry can arrive AT the successor.                                *)
(*                                                                         *)
(* Two CONSTANT toggles make the mechanism falsifiable:                       *)
(*   UseOpIdTable = TRUE  -- the leader records applied opIds in DURABLE state *)
(*       recovered across failover; a retry with a seen opId is a no-op       *)
(*       (exactly-once). This is the design's target mechanism.               *)
(*   UseOpIdTable = FALSE -- structural-only: there is no durable opId table.  *)
(*       Dedup relies on in-memory state, which a failover drops; the retry   *)
(*       then RE-APPLIES and allocates a DUPLICATE resource.                  *)
(*                                                                         *)
(*   Retention (a CONSTANT) bounds how many of the most-recently-applied      *)
(*       opIds the durable table keeps. If Retention < the retry horizon, a   *)
(*       late retry whose opId was already evicted double-applies EVEN WITH    *)
(*       the table on -- the section-15 "idempotency retention is a lower      *)
(*       bound for safe truncation" rule, made teeth.                         *)
(*                                                                         *)
(* Expected results (see the .cfg and design doc section 18):                 *)
(*   UseOpIdTable=TRUE, Retention >= MaxOps (>= retry horizon)  -> ExactlyOnce *)
(*       holds across failover + retries (the committed/faithful config).     *)
(*   UseOpIdTable=FALSE (structural-only)                       -> VIOLATED:   *)
(*       a retry across failover double-allocates.                            *)
(*   UseOpIdTable=TRUE, Retention < MaxOps                      -> VIOLATED:   *)
(*       a late retry whose opId was evicted double-allocates.                *)
(*                                                                         *)
(* NOT modeled: the actual record codec / byte log, manifest CAS, multiple    *)
(* namespaces, partial application, and WHICH concrete record families carry  *)
(* the create-op-id -- here a single abstract "allocate" op stands in for any *)
(* mutating op with an at-most-once allocation side effect.                   *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
  Ops,            \* set of distinct client operation ids (model values)
  MaxApply,       \* bound: total number of apply-effects allowed (state cap)
  Retention,      \* durable dedup table keeps the last `Retention` applied opIds
  UseOpIdTable    \* TRUE: durable opId dedup table; FALSE: structural-only

VARIABLES
  applied,        \* [Ops -> Nat] how many times each op's effect has been applied
  durableTable,   \* SUBSET Ops: opIds recorded in DURABLE (survives-failover) state
  memTable,       \* SUBSET Ops: opIds the current leader instance remembers (volatile)
  applyClock,     \* logical clock: bumped on each apply; orders the dedup table
  appliedAt,      \* [Ops -> Nat] applyClock value at which each op was first applied
  pending,        \* SUBSET Ops: ops whose ack was lost and are awaiting a retry
  totalApplies    \* aux/history: total apply-effects so far (bounds the state space)

vars == <<applied, durableTable, memTable, applyClock, appliedAt,
          pending, totalApplies>>

TypeOK ==
  /\ applied \in [Ops -> 0..MaxApply]
  /\ durableTable \in SUBSET Ops
  /\ memTable \in SUBSET Ops
  /\ applyClock \in 0..MaxApply
  /\ appliedAt \in [Ops -> 0..MaxApply]
  /\ pending \in SUBSET Ops
  /\ totalApplies \in 0..MaxApply

Init ==
  /\ applied = [o \in Ops |-> 0]
  /\ durableTable = {}
  /\ memTable = {}
  /\ applyClock = 0
  /\ appliedAt = [o \in Ops |-> 0]
  /\ pending = {}
  /\ totalApplies = 0

\* The dedup view the leader consults to decide "have I seen this opId?".
\*   UseOpIdTable: durable table (recovered across failover) UNION the volatile
\*                 in-memory table -- a seen opId is deduped.
\*   ~UseOpIdTable: structural-only -- there is no durable opId table, so dedup
\*                 can only use volatile in-memory state, which failover drops.
SeenView ==
  IF UseOpIdTable THEN durableTable \cup memTable ELSE memTable

\* Apply an op for the first time (or after its dedup record is gone): allocate
\* the fresh resource (bump `applied`), record the opId in BOTH the volatile
\* in-memory table and -- when the table is enabled -- the durable table, and
\* mark the ack as lost so the client will retry the SAME opId.
\* BOUNDED RETENTION: the durable table keeps only the most-recently-applied
\* `Retention` opIds; applying a new op evicts any whose appliedAt is more than
\* `Retention` clock-ticks behind the new tick.
ApplyOp(o) ==
  /\ totalApplies < MaxApply
  /\ o \notin SeenView
  /\ applyClock' = applyClock + 1
  /\ applied' = [applied EXCEPT ![o] = applied[o] + 1]
  /\ appliedAt' = [appliedAt EXCEPT ![o] = applyClock + 1]
  /\ memTable' = memTable \cup {o}
  /\ durableTable' =
       IF UseOpIdTable
         THEN LET inserted == durableTable \cup {o}
              IN { p \in inserted :
                     IF p = o THEN TRUE
                     ELSE appliedAt[p] > (applyClock + 1) - Retention }
         ELSE durableTable
  /\ pending' = pending \cup {o}
  /\ totalApplies' = totalApplies + 1
  /\ UNCHANGED <<>>

\* The op's effect is applied but the ACK was lost: the client re-delivers the
\* SAME opId. If the leader still recognizes the opId (SeenView), it is a no-op
\* (exactly-once); otherwise it falls through to ApplyOp and DOUBLE-applies.
\* This action models the dedup decision on a retry explicitly: a deduped retry
\* returns the prior result with no new effect.
RetryDeduped(o) ==
  /\ o \in pending
  /\ o \in SeenView
  /\ UNCHANGED vars   \* recognized retry: same result, no duplicate effect

\* A retry that the leader does NOT recognize re-applies the op. Enabled only
\* when the opId is genuinely not in the dedup view (e.g. dropped by failover
\* under structural-only, or evicted by bounded retention).
RetryReapply(o) ==
  /\ o \in pending
  /\ o \notin SeenView
  /\ totalApplies < MaxApply
  /\ applyClock' = applyClock + 1
  /\ applied' = [applied EXCEPT ![o] = applied[o] + 1]
  /\ appliedAt' = [appliedAt EXCEPT ![o] = applyClock + 1]
  /\ memTable' = memTable \cup {o}
  /\ durableTable' =
       IF UseOpIdTable
         THEN LET inserted == durableTable \cup {o}
              IN { p \in inserted :
                     IF p = o THEN TRUE
                     ELSE appliedAt[p] > (applyClock + 1) - Retention }
         ELSE durableTable
  /\ totalApplies' = totalApplies + 1
  /\ UNCHANGED <<pending>>

\* Leader failover: the current leader instance is lost and a successor takes
\* over. The successor restores DURABLE state (durableTable persists) but its
\* in-memory dedup state starts empty (memTable is dropped). This is the exact
\* failover window where the missing durable table bites: a pending retry that
\* arrives at the successor is deduped only if the opId is in durableTable.
Failover ==
  /\ memTable' = {}
  /\ UNCHANGED <<applied, durableTable, applyClock, appliedAt, pending, totalApplies>>

Next ==
  \E o \in Ops :
    \/ ApplyOp(o)
    \/ RetryDeduped(o)
    \/ RetryReapply(o)
    \/ Failover

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* SAFETY: each distinct client opId's at-most-once effect is applied AT MOST
\* ONCE -- no duplicate allocation, even across a failover that drops volatile
\* dedup state and a retry of a lost-ack op. This is the property the design's
\* claimed bounded operation-id table is supposed to provide. It FAILS under
\* structural-only (UseOpIdTable=FALSE) and under a too-short retention.
ExactlyOnce ==
  \A o \in Ops : applied[o] <= 1

\* Sanity: the durable dedup table never exceeds the retention bound. (Vacuous
\* when UseOpIdTable=FALSE, where the durable table stays empty -- that is the
\* whole point: there is no table to retain anything.)
RetentionBounded ==
  Cardinality(durableTable) <= Retention

\* Sanity: an opId is recorded as durably-deduped only if its effect was
\* actually applied (no phantom dedup entries).
DurableImpliesApplied ==
  \A o \in durableTable : applied[o] >= 1

=============================================================================
