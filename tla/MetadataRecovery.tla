---------------------------- MODULE MetadataRecovery ----------------------------
(***************************************************************************)
(* Models the scalable metadata design for one namespace. The model keeps   *)
(* the state intentionally small: a namespace metadata log, a compacted      *)
(* snapshot prefix, derived node indexes, and a leader lifecycle.            *)
(*                                                                         *)
(* The authoritative metadata log may contain only file/chunk metadata       *)
(* transition records. Derived state such as node -> chunks is rebuilt from  *)
(* the authoritative state and must never become an independent source of    *)
(* truth. A leader may serve requests only after the recovery barrier has    *)
(* loaded the snapshot, replayed every durable log record, and rebuilt       *)
(* indexes. Metadata epochs model the compact consensus fencing state:       *)
(* recovery with a stale epoch is rejected before it can fence the current   *)
(* open tail or serve metadata.                                             *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Files, Nodes, MaxLog

None == "None"
Status == {"Absent", "Live", "Deleting", "Deleted"}
Phase == {"Standby", "Recovering", "Active", "Fenced"}

AuthoritativeRecords ==
  { [kind |-> "Create", file |-> f, node |-> n] : f \in Files, n \in Nodes }
  \cup { [kind |-> "DeleteStart", file |-> f, node |-> None] : f \in Files }
  \cup { [kind |-> "DeleteFinish", file |-> f, node |-> None] : f \in Files }
  \cup { [kind |-> "Swap", file |-> f, from |-> a, to |-> b] :
            f \in Files, a \in Nodes, b \in Nodes }
  \cup { [kind |-> "Drop", file |-> f, node |-> n] : f \in Files, n \in Nodes }
  \cup { [kind |-> "Add", file |-> f, node |-> n] : f \in Files, n \in Nodes }
  \cup { [kind |-> "Sweep", file |-> f, node |-> None] : f \in Files }

EmptyState ==
  [ status |-> [f \in Files |-> "Absent"],
    replicas |-> [f \in Files |-> {}] ]

ApplyRecord(st, rec) ==
  IF rec.kind = "Create" THEN
    [st EXCEPT !.status[rec.file] = "Live",
               !.replicas[rec.file] = {rec.node}]
  ELSE IF rec.kind = "DeleteStart" THEN
    [st EXCEPT !.status[rec.file] = "Deleting"]
  ELSE IF rec.kind = "DeleteFinish" THEN
    [st EXCEPT !.status[rec.file] = "Deleted",
               !.replicas[rec.file] = {}]
  ELSE IF rec.kind = "Swap" THEN
    [st EXCEPT !.replicas[rec.file] =
        (st.replicas[rec.file] \ {rec.from}) \cup {rec.to}]
  ELSE IF rec.kind = "Drop" THEN
    [st EXCEPT !.replicas[rec.file] = st.replicas[rec.file] \ {rec.node}]
  ELSE IF rec.kind = "Add" THEN
    [st EXCEPT !.replicas[rec.file] = st.replicas[rec.file] \cup {rec.node}]
  ELSE
    [st EXCEPT !.status[rec.file] = "Absent",
               !.replicas[rec.file] = {}]

RECURSIVE ApplySeq(_, _)
ApplySeq(st, s) ==
  IF Len(s) = 0 THEN st
  ELSE ApplySeq(ApplyRecord(st, Head(s)), Tail(s))

Prefix(s, n) ==
  IF n = 0 THEN <<>> ELSE SubSeq(s, 1, n)

SuffixFrom(s, first) ==
  IF first > Len(s) THEN <<>> ELSE SubSeq(s, first, Len(s))

NodeIndex(st) ==
  [n \in Nodes |-> {f \in Files : st.status[f] \in {"Live", "Deleting"} /\ n \in st.replicas[f]}]

ValidTransition(st, rec) ==
  IF rec.kind = "Create" THEN
    st.status[rec.file] = "Absent"
  ELSE IF rec.kind = "DeleteStart" THEN
    st.status[rec.file] \in {"Live", "Deleting"}
  ELSE IF rec.kind = "DeleteFinish" THEN
    st.status[rec.file] \in {"Deleting", "Deleted"}
  ELSE IF rec.kind = "Swap" THEN
    /\ st.status[rec.file] = "Live"
    /\ rec.from \in st.replicas[rec.file]
    /\ rec.to \notin st.replicas[rec.file]
  ELSE IF rec.kind = "Drop" THEN
    /\ st.status[rec.file] = "Live"
    /\ rec.node \in st.replicas[rec.file]
  ELSE IF rec.kind = "Add" THEN
    /\ st.status[rec.file] = "Live"
    /\ rec.node \notin st.replicas[rec.file]
  ELSE
    st.status[rec.file] = "Deleted"

VARIABLES
  log,             \* Seq(AuthoritativeRecords)
  snapshotOffset,  \* number of log records included in the compacted snapshot
  snapshotState,   \* authoritative state at snapshotOffset
  manifestEpoch,   \* metadata epoch published in the compact manifest root
  leaderEpoch,     \* metadata epoch owned by the recovering/active leader
  phase,           \* Standby | Recovering | Active | Fenced
  replayOffset,    \* last log offset applied into memState during recovery
  memState,        \* active/recovering authoritative state
  nodeIndex,       \* derived node -> files index
  staleRejects     \* bounded count of stale recovery attempts rejected by consensus

vars == <<log, snapshotOffset, snapshotState, manifestEpoch, leaderEpoch,
          phase, replayOffset, memState, nodeIndex, staleRejects>>

StateType ==
  [ status : [Files -> Status],
    replicas : [Files -> SUBSET Nodes] ]

TypeOK ==
  /\ log \in Seq(AuthoritativeRecords)
  /\ Len(log) <= MaxLog
  /\ snapshotOffset \in 0..Len(log)
  /\ snapshotState \in StateType
  /\ manifestEpoch \in 0..MaxLog
  /\ leaderEpoch \in 0..MaxLog
  /\ phase \in Phase
  /\ replayOffset \in snapshotOffset..Len(log)
  /\ memState \in StateType
  /\ nodeIndex \in [Nodes -> SUBSET Files]
  /\ staleRejects \in 0..MaxLog

Init ==
  /\ log = <<>>
  /\ snapshotOffset = 0
  /\ snapshotState = EmptyState
  /\ manifestEpoch = 0
  /\ leaderEpoch = 0
  /\ phase = "Standby"
  /\ replayOffset = 0
  /\ memState = EmptyState
  /\ nodeIndex = NodeIndex(EmptyState)
  /\ staleRejects = 0

AppendAuthoritativeRecord ==
  \E rec \in AuthoritativeRecords :
    /\ phase = "Active"
    /\ leaderEpoch = manifestEpoch
    /\ Len(log) < MaxLog
    /\ ValidTransition(memState, rec)
    /\ log' = Append(log, rec)
    /\ memState' = ApplyRecord(memState, rec)
    /\ nodeIndex' = NodeIndex(memState')
    /\ replayOffset' = Len(log')
    /\ UNCHANGED <<snapshotOffset, snapshotState, manifestEpoch, leaderEpoch,
                  phase, staleRejects>>

StartRecovery ==
  \E e \in 0..MaxLog :
    /\ phase \in {"Standby", "Fenced"}
    /\ e >= manifestEpoch
    /\ leaderEpoch' = e
    /\ phase' = "Recovering"
    /\ replayOffset' = snapshotOffset
    /\ memState' = snapshotState
    /\ nodeIndex' = NodeIndex(snapshotState)
    /\ UNCHANGED <<log, snapshotOffset, snapshotState, manifestEpoch, staleRejects>>

RejectStaleRecovery ==
  \E e \in 0..MaxLog :
    /\ phase \in {"Standby", "Fenced"}
    /\ e < manifestEpoch
    /\ staleRejects < MaxLog
    /\ staleRejects' = staleRejects + 1
    /\ UNCHANGED <<log, snapshotOffset, snapshotState, manifestEpoch,
                  leaderEpoch, phase, replayOffset, memState, nodeIndex>>

ExternalEpochBump ==
  /\ phase \in {"Standby", "Fenced"}
  /\ manifestEpoch < MaxLog
  /\ manifestEpoch' = manifestEpoch + 1
  /\ UNCHANGED <<log, snapshotOffset, snapshotState, leaderEpoch,
                phase, replayOffset, memState, nodeIndex, staleRejects>>

ReplayOne ==
  /\ phase = "Recovering"
  /\ replayOffset < Len(log)
  /\ LET rec == log[replayOffset + 1] IN
       /\ memState' = ApplyRecord(memState, rec)
       /\ replayOffset' = replayOffset + 1
       /\ nodeIndex' = NodeIndex(memState')
  /\ UNCHANGED <<log, snapshotOffset, snapshotState, manifestEpoch,
                leaderEpoch, phase, staleRejects>>

BecomeActive ==
  /\ phase = "Recovering"
  /\ replayOffset = Len(log)
  /\ nodeIndex = NodeIndex(memState)
  /\ phase' = "Active"
  /\ manifestEpoch' = leaderEpoch
  /\ UNCHANGED <<log, snapshotOffset, snapshotState, leaderEpoch,
                replayOffset, memState, nodeIndex, staleRejects>>

LoseLeadership ==
  /\ phase \in {"Active", "Recovering"}
  /\ phase' = "Fenced"
  /\ UNCHANGED <<log, snapshotOffset, snapshotState, manifestEpoch, leaderEpoch,
                replayOffset, memState, nodeIndex, staleRejects>>

CompactSnapshot ==
  /\ phase = "Active"
  /\ leaderEpoch = manifestEpoch
  /\ \E cut \in snapshotOffset..Len(log) :
       /\ snapshotOffset' = cut
       /\ snapshotState' = ApplySeq(EmptyState, Prefix(log, cut))
  /\ UNCHANGED <<log, manifestEpoch, leaderEpoch, phase,
                replayOffset, memState, nodeIndex, staleRejects>>

Next ==
  \/ AppendAuthoritativeRecord
  \/ StartRecovery
  \/ RejectStaleRecovery
  \/ ExternalEpochBump
  \/ ReplayOne
  \/ BecomeActive
  \/ LoseLeadership
  \/ CompactSnapshot

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

FullState ==
  ApplySeq(EmptyState, log)

RecoveredFromSnapshot ==
  ApplySeq(snapshotState, SuffixFrom(log, snapshotOffset + 1))

LogOnlyAuthoritative ==
  \A i \in 1..Len(log) : log[i] \in AuthoritativeRecords

SnapshotMatchesPrefix ==
  snapshotState = ApplySeq(EmptyState, Prefix(log, snapshotOffset))

CompactionPreservesAuthoritativeState ==
  RecoveredFromSnapshot = FullState

ActiveAfterFullReplay ==
  phase = "Active" =>
    /\ replayOffset = Len(log)
    /\ memState = FullState
    /\ nodeIndex = NodeIndex(FullState)

StaleEpochCannotServe ==
  phase \in {"Recovering", "Active"} => leaderEpoch >= manifestEpoch

ActiveLeaderEpochCurrent ==
  phase = "Active" => leaderEpoch = manifestEpoch

DerivedIndexRebuildable ==
  phase = "Active" => nodeIndex = NodeIndex(ApplySeq(snapshotState, SuffixFrom(log, snapshotOffset + 1)))

DeletedTombstonesCompact ==
  \A f \in Files : FullState.status[f] = "Deleted" => FullState.replicas[f] = {}

=============================================================================
