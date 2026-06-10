---------------------------- MODULE ChunkReplication ----------------------------
(***************************************************************************)
(* Models the correctness core of Strata's chunk protocol (tech design     *)
(* §5, §7.3): a single chunk, 3 replicas, 2-of-3 quorum acknowledgment,    *)
(* per-replica epoch fencing, and seal recovery by a new epoch racing a    *)
(* zombie writer, with at most one replica crash (fail-stop, unreachable). *)
(*                                                                         *)
(* The epoch-1 writer appends records 1..MaxApp in order (contiguity       *)
(* enforced replica-side, as in ChunkStore). Append messages float in the  *)
(* network and may be delivered at any time — including after recovery     *)
(* fenced some replicas (the zombie race). Producer acks are derived:      *)
(* record i is ack-able whenever >=2 replicas hold it. Recovery fences     *)
(* replicas one at a time (interleaving allowed), then seals at the        *)
(* longest prefix found on >=2 fenced reachable replicas, re-replicating  *)
(* missing entries to them.                                                *)
(*                                                                         *)
(* Invariants:                                                             *)
(*   NoAckedLoss      — every producer-acked record is within the seal     *)
(*   PrefixConsistent — replica logs never diverge (rlog[r][j] = j)        *)
(*   SealedAgree      — sealed replicas all have exactly the sealed prefix *)
(*                                                                         *)
(* The 2-of-3 pigeonhole (any ack pair intersects any fence pair) is what  *)
(* makes NoAckedLoss hold — TLC explores every interleaving to confirm.    *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Replicas, MaxApp

E1 == 1                    \* the original writer's epoch
E2 == 2                    \* the recovery epoch
NoFence == -1

VARIABLES
  rlog,      \* [Replicas -> Seq(Nat)]   replica logs (record ids)
  rfence,    \* [Replicas -> Int]        persisted fence epoch
  rsealed,   \* [Replicas -> BOOLEAN]    replica-side sealed flag
  up,        \* [Replicas -> BOOLEAN]    reachable (fail-stop crash)
  msgs,      \* in-flight appends: [dst, epoch, pos, rec]
  nextRec,   \* next record the epoch-1 writer sends (writer may be a zombie)
  acked,     \* set of producer-acked record ids
  sealedLen, \* -1 until recovery seals the chunk
  crashes    \* number of crashes so far (bounded to 1)

vars == <<rlog, rfence, rsealed, up, msgs, nextRec, acked, sealedLen, crashes>>

TypeOK ==
  /\ rlog \in [Replicas -> Seq(1..MaxApp)]
  /\ rfence \in [Replicas -> {NoFence, E2}]
  /\ rsealed \in [Replicas -> BOOLEAN]
  /\ up \in [Replicas -> BOOLEAN]
  /\ nextRec \in 1..(MaxApp + 1)
  /\ acked \subseteq 1..MaxApp
  /\ sealedLen \in {-1} \cup (0..MaxApp)
  /\ crashes \in 0..1

Init ==
  /\ rlog = [r \in Replicas |-> <<>>]
  /\ rfence = [r \in Replicas |-> NoFence]
  /\ rsealed = [r \in Replicas |-> FALSE]
  /\ up = [r \in Replicas |-> TRUE]
  /\ msgs = {}
  /\ nextRec = 1
  /\ acked = {}
  /\ sealedLen = -1
  /\ crashes = 0

(* The epoch-1 writer sends the next record to all replicas. It never learns it
   was deposed — it keeps sending (zombie behavior is the point of the model). *)
WriterSend ==
  /\ nextRec <= MaxApp
  /\ msgs' = msgs \cup {[dst |-> r, epoch |-> E1, pos |-> nextRec - 1, rec |-> nextRec] : r \in Replicas}
  /\ nextRec' = nextRec + 1
  /\ UNCHANGED <<rlog, rfence, rsealed, up, acked, sealedLen, crashes>>

(* A replica receives an append: exactly ChunkStore.append's rules —
   epoch >= fence, not sealed, contiguous position. Rejections drop the message. *)
DeliverAppend ==
  \E m \in msgs :
    /\ up[m.dst]
    /\ msgs' = msgs \ {m}
    /\ IF /\ m.epoch >= rfence[m.dst]
          /\ ~rsealed[m.dst]
          /\ Len(rlog[m.dst]) = m.pos
       THEN rlog' = [rlog EXCEPT ![m.dst] = Append(@, m.rec)]
       ELSE rlog' = rlog
    /\ UNCHANGED <<rfence, rsealed, up, nextRec, acked, sealedLen, crashes>>

(* A message may be lost. *)
DropMsg ==
  \E m \in msgs :
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED <<rlog, rfence, rsealed, up, nextRec, acked, sealedLen, crashes>>

(* Producer ack: record i is acked once >=2 replicas hold it (over-approximates
   ack messages; sound for safety checking). The zombie may keep acking — the
   invariant must hold anyway. *)
ProducerAck ==
  \E i \in 1..MaxApp :
    /\ i \notin acked
    /\ Cardinality({r \in Replicas : Len(rlog[r]) >= i}) >= 2
    /\ acked' = acked \cup {i}
    /\ UNCHANGED <<rlog, rfence, rsealed, up, msgs, nextRec, sealedLen, crashes>>

(* Fail-stop crash of one replica: it becomes permanently unreachable. *)
Crash ==
  /\ crashes = 0
  /\ \E r \in Replicas :
       /\ up[r]
       /\ up' = [up EXCEPT ![r] = FALSE]
       /\ crashes' = 1
       /\ UNCHANGED <<rlog, rfence, rsealed, msgs, nextRec, acked, sealedLen>>

(* Recovery fences one reachable replica at a time — zombie deliveries interleave. *)
FenceOne ==
  /\ sealedLen = -1
  /\ \E r \in Replicas :
       /\ up[r] /\ rfence[r] = NoFence
       /\ rfence' = [rfence EXCEPT ![r] = E2]
       /\ UNCHANGED <<rlog, rsealed, up, msgs, nextRec, acked, sealedLen, crashes>>

(* Once >=2 reachable replicas are fenced, recovery seals: the durable prefix is
   the LONGEST log among the fenced reachable replicas (anything found anywhere
   reachable is preserved — §7.3 step 3); shorter fenced replicas are completed
   by re-replication, then all fenced reachable replicas seal at that length. *)
FinishRecovery ==
  /\ sealedLen = -1
  /\ LET fenced == {r \in Replicas : up[r] /\ rfence[r] = E2}
     IN /\ Cardinality(fenced) >= 2
        /\ LET p == LET lens == {Len(rlog[r]) : r \in fenced}
                    IN CHOOSE l \in lens : \A l2 \in lens : l >= l2
           IN /\ sealedLen' = p
              /\ rlog' = [r \in Replicas |->
                            IF r \in fenced THEN [j \in 1..p |-> j] ELSE rlog[r]]
              /\ rsealed' = [r \in Replicas |-> IF r \in fenced THEN TRUE ELSE rsealed[r]]
        /\ UNCHANGED <<rfence, up, msgs, nextRec, acked, crashes>>

Next ==
  \/ WriterSend
  \/ DeliverAppend
  \/ DropMsg
  \/ ProducerAck
  \/ Crash
  \/ FenceOne
  \/ FinishRecovery

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

(* Single writer + contiguity + fencing => no replica ever diverges from the
   canonical history 1,2,3,...  *)
PrefixConsistent ==
  \A r \in Replicas : \A j \in 1..Len(rlog[r]) : rlog[r][j] = j

(* THE safety property: nothing the producer was told is durable can ever sit
   beyond the recovery seal. This is the 2-of-3 intersection argument:
   any ack pair shares a member with any fence pair. *)
NoAckedLoss ==
  sealedLen >= 0 => \A i \in acked : i <= sealedLen

(* Sealed replicas hold exactly the sealed prefix. *)
SealedAgree ==
  \A r \in Replicas : rsealed[r] => Len(rlog[r]) = sealedLen

(* Acked data also remains physically present on >=1 reachable replica
   (repairability) as long as at most one crash occurred. *)
AckedSurvivable ==
  \A i \in acked : Cardinality({r \in Replicas : up[r] /\ Len(rlog[r]) >= i}) >= 1

================================================================================
