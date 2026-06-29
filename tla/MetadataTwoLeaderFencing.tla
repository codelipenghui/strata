-------------------------- MODULE MetadataTwoLeaderFencing --------------------------
(***************************************************************************)
(* Two-leader extension of MetadataRecovery, for ONE namespace.            *)
(*                                                                         *)
(* This models the failover OVERLAP window that the single-leader model     *)
(* cannot express: an old leader (term eA) and a successor (term eB > eA)    *)
(* of the SAME namespace, where the old leader may still be appending to     *)
(* the open metadata-log tail while the successor recovers. It does NOT      *)
(* describe a design that allows two leaders; it exists only to PROVE that   *)
(* the epoch + fence/seal/roll machinery keeps the namespace single-writer   *)
(* and loses no acknowledged record across the overlap (sections 8.1, 9,     *)
(* 13, 15 of strata-metadata-scaling-design.md).                            *)
(*                                                                         *)
(* Storage model: the durable log is committed \o openTail. openTail is the  *)
(* current open chunk. Only the leader whose epoch OWNS the chunk may append, *)
(* and only while it is not fence-sealed -- this is the storage-layer fence.  *)
(* Recovery draws a STRICTLY greater epoch (CAS-increment of the consensus    *)
(* counter, fixing the >= simplification in the single-leader model),         *)
(* fence-seals the old open chunk at its durable end, folds the sealed bytes  *)
(* into committed, and rolls to a fresh chunk it owns.                       *)
(*                                                                         *)
(* Records are opaque payloads: the fencing safety is independent of record  *)
(* semantics, which the single-leader model already covers.                  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Leaders, Records, MaxLog, MaxEpoch

Phase == {"Standby", "Recovering", "Active", "Fenced"}

VARIABLES
  committed,     \* Seq(Records): durable, sealed, agreed log prefix
  openTail,      \* Seq(Records): records in the current open chunk
  openOwner,     \* epoch allowed to append to the open chunk (0 = none)
  openSealed,    \* BOOLEAN: is the open chunk fence-sealed?
  sealedLen,     \* durable end captured at fence-seal time
  epochCounter,  \* /strata/meta/epoch CAS counter (monotonic, strictly inc)
  manifestEpoch, \* epoch in the published manifest
  phase,         \* [Leaders -> Phase]
  leaderEpoch,   \* [Leaders -> Nat] epoch owned by each (would-be) leader
  memState,      \* [Leaders -> Seq(Records)] each leader's applied view
  ackedLog       \* Seq(Records): every record a leader has acked (history aux)

vars == <<committed, openTail, openOwner, openSealed, sealedLen,
          epochCounter, manifestEpoch, phase, leaderEpoch, memState, ackedLog>>

DurableLog == committed \o openTail

TypeOK ==
  /\ committed \in Seq(Records)
  /\ openTail \in Seq(Records)
  /\ openOwner \in 0..MaxEpoch
  /\ openSealed \in BOOLEAN
  /\ sealedLen \in 0..MaxLog
  /\ epochCounter \in 0..MaxEpoch
  /\ manifestEpoch \in 0..MaxEpoch
  /\ phase \in [Leaders -> Phase]
  /\ leaderEpoch \in [Leaders -> 0..MaxEpoch]
  /\ memState \in [Leaders -> Seq(Records)]
  /\ ackedLog \in Seq(Records)
  /\ Len(DurableLog) <= MaxLog

Init ==
  /\ committed = <<>>
  /\ openTail = <<>>
  /\ openOwner = 0
  /\ openSealed = FALSE
  /\ sealedLen = 0
  /\ epochCounter = 0
  /\ manifestEpoch = 0
  /\ phase = [l \in Leaders |-> "Standby"]
  /\ leaderEpoch = [l \in Leaders |-> 0]
  /\ memState = [l \in Leaders |-> <<>>]
  /\ ackedLog = <<>>

\* A leader wins a new term: CAS-increment the epoch counter to a STRICTLY
\* greater epoch. Does NOT touch the open tail yet -- the prior leader may
\* still be Active and appending. This is the start of the overlap window.
StartRecovery(l) ==
  /\ phase[l] \in {"Standby", "Fenced"}
  /\ epochCounter < MaxEpoch
  /\ epochCounter' = epochCounter + 1
  /\ leaderEpoch' = [leaderEpoch EXCEPT ![l] = epochCounter + 1]
  /\ phase' = [phase EXCEPT ![l] = "Recovering"]
  /\ UNCHANGED <<committed, openTail, openOwner, openSealed, sealedLen,
                manifestEpoch, memState, ackedLog>>

\* Fence + seal the old open chunk at its current durable end. Only the
\* latest-term recovering leader proceeds; an older recovering leader whose
\* epoch is no longer the counter is stuck (superseded) -- that IS the fence.
FenceSeal(l) ==
  /\ phase[l] = "Recovering"
  /\ leaderEpoch[l] = epochCounter
  /\ ~openSealed
  /\ openSealed' = TRUE
  /\ sealedLen' = Len(openTail)
  /\ UNCHANGED <<committed, openTail, openOwner, epochCounter, manifestEpoch,
                phase, leaderEpoch, memState, ackedLog>>

\* Recover the sealed durable prefix, fold it into committed, and roll to a
\* fresh open chunk owned by this leader's epoch (section 8.1 fencing rule).
FoldAndRoll(l) ==
  /\ phase[l] = "Recovering"
  /\ leaderEpoch[l] = epochCounter
  /\ openSealed
  /\ committed' = committed \o SubSeq(openTail, 1, sealedLen)
  /\ openTail' = <<>>
  /\ openOwner' = leaderEpoch[l]
  /\ openSealed' = FALSE
  /\ sealedLen' = 0
  /\ memState' = [memState EXCEPT ![l] = committed \o SubSeq(openTail, 1, sealedLen)]
  /\ UNCHANGED <<epochCounter, manifestEpoch, phase, leaderEpoch, ackedLog>>

\* Publish the manifest at this epoch and start serving.
BecomeActive(l) ==
  /\ phase[l] = "Recovering"
  /\ leaderEpoch[l] = epochCounter
  /\ openOwner = leaderEpoch[l]
  /\ ~openSealed
  /\ phase' = [phase EXCEPT ![l] = "Active"]
  /\ manifestEpoch' = leaderEpoch[l]
  /\ UNCHANGED <<committed, openTail, openOwner, openSealed, sealedLen,
                epochCounter, leaderEpoch, memState, ackedLog>>

\* Active owner appends a record. Storage fence: it must own the open chunk
\* and the chunk must be open. This single guard is what makes a stale leader
\* harmless even while it still believes it is Active.
AppendRecord(l) ==
  /\ phase[l] = "Active"
  /\ ~openSealed
  /\ leaderEpoch[l] = openOwner
  /\ Len(DurableLog) < MaxLog
  /\ \E r \in Records :
       /\ openTail' = Append(openTail, r)
       /\ memState' = [memState EXCEPT ![l] = Append(memState[l], r)]
       /\ ackedLog' = Append(ackedLog, r)
  /\ UNCHANGED <<committed, openOwner, openSealed, sealedLen, epochCounter,
                manifestEpoch, phase, leaderEpoch>>

\* Crash or step down. The durable log persists; a successor must fence+roll
\* before it can write. A Fenced leader can later re-run recovery (flap).
LoseLeadership(l) ==
  /\ phase[l] \in {"Active", "Recovering"}
  /\ phase' = [phase EXCEPT ![l] = "Fenced"]
  /\ UNCHANGED <<committed, openTail, openOwner, openSealed, sealedLen,
                epochCounter, manifestEpoch, leaderEpoch, memState, ackedLog>>

Next ==
  \E l \in Leaders :
    \/ StartRecovery(l)
    \/ FenceSeal(l)
    \/ FoldAndRoll(l)
    \/ BecomeActive(l)
    \/ AppendRecord(l)
    \/ LoseLeadership(l)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* The leader can currently write the open chunk (the single-writer gate).
\* NOTE two leaders may BOTH be phase = "Active" during the overlap; what
\* must be unique is who can actually write.
CanWrite(l) ==
  /\ phase[l] = "Active"
  /\ ~openSealed
  /\ leaderEpoch[l] = openOwner

\* SAFETY 1: no acknowledged record is ever lost, and none appears that was
\* never acked. committed \o openTail is exactly the acked history -- a short
\* seal or a stale-leader append after fencing would break this.
NoAckedRecordLost ==
  DurableLog = ackedLog

\* SAFETY 2: at most one leader can write the open chunk at any instant, even
\* during the failover overlap -- the namespace stays single-writer.
SingleWriter ==
  \A l1, l2 \in Leaders : (CanWrite(l1) /\ CanWrite(l2)) => l1 = l2

\* SAFETY 3: the writing owner's epoch equals the published manifest epoch; a
\* superseded old leader cannot be the owner-writer.
WriterEpochCurrent ==
  \A l \in Leaders : CanWrite(l) => leaderEpoch[l] = manifestEpoch

\* SAFETY 4: the writing owner's in-memory state equals the full durable log
\* (the recovery-barrier "appliedOffset == durable end" property).
WriterMemReflectsLog ==
  \A l \in Leaders : CanWrite(l) => memState[l] = DurableLog

\* Sanity: the consensus counter dominates every owned/published epoch.
EpochCounterDominates ==
  /\ manifestEpoch <= epochCounter
  /\ openOwner <= epochCounter
  /\ \A l \in Leaders : leaderEpoch[l] <= epochCounter

=============================================================================
