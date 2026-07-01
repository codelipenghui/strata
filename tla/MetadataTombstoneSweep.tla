-------------------------- MODULE MetadataTombstoneSweep --------------------------
(***************************************************************************)
(* Models the tombstone-sweep retention safety condition for one deleted    *)
(* file/path in one namespace (retention safety, §4 of                      *)
(* strata-tech-design.md).                                                  *)
(*                                                                         *)
(* The design states a TWO-PART condition for emitting TombstoneSwept:       *)
(*   (1) the retention window has elapsed, AND                              *)
(*   (2) the tombstone is already in a PUBLISHED snapshot,                  *)
(* so that stale create/seal/delete retries can no longer resurrect the old *)
(* file id or path binding. The current code enforces only a WALL-CLOCK     *)
(* form of (1): nowMs - deletedAtMs >= retention, using a non-monotonic      *)
(* System.currentTimeMillis() and no published-snapshot check.              *)
(*                                                                         *)
(* This model has two clocks: realTime (ground-truth, monotonic) and        *)
(* wallClock (what the leader reads; can be skewed/jump via ClockJump). A    *)
(* stale pre-delete retry can still arrive within RetryHorizon of the        *)
(* deletion in REAL time; while the tombstone (fence) exists the retry is    *)
(* rejected, but once swept it resurrects the file.                         *)
(*                                                                         *)
(* SafeSweep = TRUE  models the design's full two-part condition.            *)
(* SafeSweep = FALSE models the current code rule (wall-clock only).         *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS MaxTime, Retention, RetryHorizon, SafeSweep

Status == {"Absent", "Live", "Deleted", "Swept"}

VARIABLES
  realTime,        \* ground-truth monotonic time (only advances)
  wallClock,       \* System.currentTimeMillis(): tracks realTime but can skew
  status,          \* file lifecycle for the slot
  deletedAtWall,   \* wallClock captured at delete (the stored deletedAtMs)
  deletedAtReal,   \* realTime at delete (ground truth; used only by invariants)
  tombPublished,   \* has the FileDeleted tombstone been put in a published snapshot?
  resurrected      \* did a stale pre-delete retry slip through after sweep?

vars == <<realTime, wallClock, status, deletedAtWall, deletedAtReal,
          tombPublished, resurrected>>

TypeOK ==
  /\ realTime \in 0..MaxTime
  /\ wallClock \in 0..MaxTime
  /\ status \in Status
  /\ deletedAtWall \in 0..MaxTime
  /\ deletedAtReal \in 0..MaxTime
  /\ tombPublished \in BOOLEAN
  /\ resurrected \in BOOLEAN

Init ==
  /\ realTime = 0
  /\ wallClock = 0
  /\ status = "Absent"
  /\ deletedAtWall = 0
  /\ deletedAtReal = 0
  /\ tombPublished = FALSE
  /\ resurrected = FALSE

\* Real time advances; the wall clock advances with it (the well-behaved case).
Tick ==
  /\ realTime < MaxTime
  /\ wallClock < MaxTime
  /\ realTime' = realTime + 1
  /\ wallClock' = wallClock + 1
  /\ UNCHANGED <<status, deletedAtWall, deletedAtReal, tombPublished, resurrected>>

\* The wall clock jumps to any value (NTP correction, VM pause, leap second).
\* realTime is unaffected -- this is the non-monotonic clock the code trusts.
ClockJump ==
  /\ \E w \in 0..MaxTime : wallClock' = w
  /\ UNCHANGED <<realTime, status, deletedAtWall, deletedAtReal, tombPublished, resurrected>>

Create ==
  /\ status = "Absent"
  /\ status' = "Live"
  /\ UNCHANGED <<realTime, wallClock, deletedAtWall, deletedAtReal, tombPublished, resurrected>>

Delete ==
  /\ status = "Live"
  /\ status' = "Deleted"
  /\ deletedAtWall' = wallClock
  /\ deletedAtReal' = realTime
  /\ tombPublished' = FALSE
  /\ UNCHANGED <<realTime, wallClock, resurrected>>

\* The tombstone is captured into a published snapshot.
PublishTombstone ==
  /\ status = "Deleted"
  /\ tombPublished' = TRUE
  /\ UNCHANGED <<realTime, wallClock, status, deletedAtWall, deletedAtReal, resurrected>>

\* Emit TombstoneSwept (remove the fence). The guard is the whole question:
\*   SafeSweep:  retention elapsed in REAL time AND tombstone published.
\*   ~SafeSweep: the code rule -- wall-clock delta only.
SweepEnabled ==
  IF SafeSweep
    THEN /\ realTime - deletedAtReal >= RetryHorizon
         /\ tombPublished
    ELSE wallClock - deletedAtWall >= Retention

Sweep ==
  /\ status = "Deleted"
  /\ SweepEnabled
  /\ status' = "Swept"
  /\ UNCHANGED <<realTime, wallClock, deletedAtWall, deletedAtReal, tombPublished, resurrected>>

\* A stale create/seal/delete retry that predates the deletion is still in
\* flight (within RetryHorizon, in REAL time). If the fence (tombstone) is
\* gone, it resurrects the file id / path binding. While status = "Deleted"
\* the fence rejects it, so this is only enabled once the tombstone is swept.
StaleResurrect ==
  /\ status = "Swept"
  /\ realTime < deletedAtReal + RetryHorizon
  /\ resurrected' = TRUE
  /\ UNCHANGED <<realTime, wallClock, status, deletedAtWall, deletedAtReal, tombPublished>>

Next ==
  \/ Tick
  \/ ClockJump
  \/ Create
  \/ Delete
  \/ PublishTombstone
  \/ Sweep
  \/ StaleResurrect

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* Leg 1 (timing): no stale pre-delete retry ever resurrects a deleted file.
NoStaleResurrection ==
  ~resurrected

\* Leg 2 (durability): a tombstone is never swept before it was captured in a
\* published snapshot -- otherwise the snapshot lineage has no fence and a
\* recovery from it loses the deletion. This is the published-snapshot half of
\* the §4 retention condition.
SweptImpliesPublished ==
  (status = "Swept") => tombPublished

=============================================================================
