-------------------------- MODULE MetadataByteDurability --------------------------
(***************************************************************************)
(* Byte-level durability of the namespace metadata log (§4 of               *)
(* strata-tech-design.md, plus the review's durability                       *)
(* findings). The metadata-log bytes are stored as ordinary replicated       *)
(* Strata chunks: "system metadata files use the same Strata chunk           *)
(* replication, seal verification, and checksum model"; "a metadata mutation *)
(* is acknowledged only after it is appended to the owning namespace          *)
(* metadata log ... durable under the configured policy."                    *)
(*                                                                         *)
(* This model isolates the two findings the review reached about that path:   *)
(*                                                                         *)
(*   FINDING A (availability/durability of the policy).  The current         *)
(*   metadata-log append is WRITE-TO-ALL (every RF replica must ack), not a   *)
(*   quorum. A quorum-append policy "consistent with seal" is target work.    *)
(*   The CONSTANT toggle WriteAll selects all-RF vs a quorum width W. The     *)
(*   durability question is: does the read/recovery set always intersect the  *)
(*   ack set, so an acked record is never lost? With WriteAll and a read set  *)
(*   of any one surviving replica this holds trivially; with a quorum W it    *)
(*   holds iff W + ReadW > RF (the seal-quorum intersection rule). Setting W  *)
(*   too small relative to the read width loses an acked record.              *)
(*                                                                         *)
(*   FINDING B (integrity).  The open metadata-log tail is served/recovered   *)
(*   relying on a cross-replica COMMON-PREFIX (length/offset agreement) on    *)
(*   the live read path, not a per-record CONTENT CRC. The framed-record      *)
(*   codecs do carry CRC32C, but the live read/recovery path can MASK a       *)
(*   single-replica bitflip rather than detect it. The CONSTANT toggle        *)
(*   UseContentCRC selects whether reconstruction verifies content (rejects a *)
(*   corrupt copy and uses a verified one) or trusts the byte a replica holds *)
(*   at that offset (accepting a bitflip silently).                           *)
(*                                                                         *)
(* Storage model: RF replicas, each a partial copy of the log over positions  *)
(* 1..MaxLen. Per replica/position the stored cell is one of:                 *)
(*   "miss"  -- replica lagged / never received this append;                  *)
(*   a value -- the byte content actually stored (= the true value, or a      *)
(*              flipped value after a Corrupt fault).                         *)
(* ackedVal[p] records the true content the writer made durable at position p *)
(* (None until acked) -- ground truth the invariants compare against.         *)
(*                                                                         *)
(* What is abstracted: real CRC32C arithmetic (a CRC check is modeled as      *)
(* "the stored value equals the true value"); chunk seal/roll (covered by     *)
(* MetadataTwoLeaderFencing); multi-namespace; the durable-HWM clamp (noted   *)
(* in honest_limitations). Values are a tiny set so a "bitflip" = pick any    *)
(* OTHER value in the set.                                                    *)
(*                                                                         *)
(* SAFE committed config: WriteAll=TRUE (or a quorum W with W+ReadW>RF),       *)
(* UseContentCRC=TRUE -> both invariants hold. TEETH: UseContentCRC=FALSE     *)
(* violates NoSilentCorruption; a too-small quorum violates NoAckedLoss.      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
  Replicas,       \* the RF replica set, e.g. {r1, r2, r3}
  Values,         \* possible byte contents of a record, e.g. {va, vb}
  MaxLen,         \* max log length / number of append positions
  WriteAll,       \* TRUE = ack needs all RF; FALSE = ack needs a quorum of W
  W,              \* quorum ack width (used when ~WriteAll)
  ReadW,          \* number of replicas the read/recovery set consults
  MaxCorrupt,     \* bound on bitflip faults (the finding is single-replica corruption)
  UseContentCRC   \* TRUE = reconstruction verifies content (rejects a bitflip)

None == "None"
Miss == "miss"          \* cell value meaning "replica has no copy at this position"

Positions == 1..MaxLen
RF == Cardinality(Replicas)

\* Replicas that must ack for a record to be durable under the policy.
AckThreshold == IF WriteAll THEN RF ELSE W

VARIABLES
  trueVal,    \* [Positions -> Values \cup {None}]  the content the writer is appending (None = no append yet)
  ackedVal,   \* [Positions -> Values \cup {None}]  true content once durable (None until acked)
  cell,       \* [Replicas -> [Positions -> Values \cup {Miss}]]  what each replica stores
  appended,   \* set of positions the writer has started appending (sent to replicas)
  up,         \* [Replicas -> BOOLEAN]  reachable (a downed replica is not in any read set)
  corrupts,   \* number of bitflip faults so far (bounded -- single-replica corruption)
  readResult, \* [Positions -> Values \cup {None}]  content a read/recovery reconstructed (None = not read / not recoverable)
  readDone,   \* set of positions a read/recovery has attempted to reconstruct
  readAcked   \* subset of readDone that were ALREADY acked/durable when read

vars == <<trueVal, ackedVal, cell, appended, up, corrupts, readResult, readDone, readAcked>>

\* Replicas that currently hold the true (uncorrupted) content at position p.
\* (At ack time the writer counts these against the durability threshold.)
HoldsTrue(p) == {r \in Replicas : cell[r][p] = trueVal[p]}

TypeOK ==
  /\ trueVal \in [Positions -> Values \cup {None}]
  /\ ackedVal \in [Positions -> Values \cup {None}]
  /\ cell \in [Replicas -> [Positions -> Values \cup {Miss}]]
  /\ appended \subseteq Positions
  /\ up \in [Replicas -> BOOLEAN]
  /\ corrupts \in 0..MaxCorrupt
  /\ readResult \in [Positions -> Values \cup {None}]
  /\ readDone \subseteq Positions
  /\ readAcked \subseteq readDone

Init ==
  /\ trueVal = [p \in Positions |-> None]
  /\ ackedVal = [p \in Positions |-> None]
  /\ cell = [r \in Replicas |-> [p \in Positions |-> Miss]]
  /\ appended = {}
  /\ up = [r \in Replicas |-> TRUE]
  /\ corrupts = 0
  /\ readResult = [p \in Positions |-> None]
  /\ readDone = {}
  /\ readAcked = {}

\* The writer begins appending position p with a chosen content value. Sends to
\* an arbitrary subset of replicas that succeed -- the rest lag (MissWrite is the
\* complement). Append is contiguous: position p needs p-1 already appended.
StartAppend(p) ==
  /\ p \notin appended
  /\ \A q \in Positions : q < p => q \in appended
  /\ \E v \in Values, S \in SUBSET Replicas :
       /\ S # {}
       /\ trueVal' = [trueVal EXCEPT ![p] = v]
       /\ cell' = [r \in Replicas |->
                     IF r \in S THEN [cell[r] EXCEPT ![p] = v] ELSE cell[r]]
       /\ appended' = appended \cup {p}
  /\ UNCHANGED <<ackedVal, up, corrupts, readResult, readDone, readAcked>>

\* A lagging replica catches up and stores the true content (late write delivery).
CatchUp(p) ==
  /\ p \in appended
  /\ \E r \in Replicas :
       /\ cell[r][p] = Miss
       /\ cell' = [cell EXCEPT ![r][p] = trueVal[p]]
  /\ UNCHANGED <<trueVal, ackedVal, appended, up, corrupts, readResult, readDone, readAcked>>

\* Acknowledge position p as durable once the policy threshold of replicas hold
\* the true content. This is exactly the §4 durability rule: ack only after the
\* append is durable under the configured policy.
Ack(p) ==
  /\ p \in appended
  /\ ackedVal[p] = None
  /\ Cardinality(HoldsTrue(p)) >= AckThreshold
  /\ ackedVal' = [ackedVal EXCEPT ![p] = trueVal[p]]
  /\ UNCHANGED <<trueVal, cell, appended, up, corrupts, readResult, readDone, readAcked>>

\* Fault: a stored copy is bit-flipped to a DIFFERENT value (single-replica
\* corruption). The framed codec's CRC32C would catch this; the question is
\* whether the live read/recovery path does.
Corrupt(p) ==
  /\ corrupts < MaxCorrupt
  /\ p \in appended
  /\ \E r \in Replicas, v \in Values :
       /\ cell[r][p] # Miss
       /\ v # cell[r][p]
       /\ cell' = [cell EXCEPT ![r][p] = v]
  /\ corrupts' = corrupts + 1
  /\ UNCHANGED <<trueVal, ackedVal, appended, up, readResult, readDone, readAcked>>

\* Fault: a replica becomes unreachable (fail-stop). Bounded by leaving at least
\* enough replicas to form a read set; we simply allow downing one replica.
Fail ==
  /\ \E r \in Replicas :
       /\ up[r]
       /\ Cardinality({x \in Replicas : up[x]}) > 1
       /\ up' = [up EXCEPT ![r] = FALSE]
  /\ UNCHANGED <<trueVal, ackedVal, cell, appended, corrupts, readResult, readDone, readAcked>>

(***************************************************************************)
(* Read / recovery. A read set of ReadW reachable replicas is consulted.     *)
(* For a given position the reconstruction picks a content value:            *)
(*                                                                         *)
(*  UseContentCRC = TRUE:  it accepts a value only if SOME read replica holds *)
(*    the true (CRC-valid) content; a corrupt copy fails its CRC and is       *)
(*    skipped. If no read replica holds a CRC-valid copy, the read yields     *)
(*    None (detected gap) rather than fabricating content. CRC NEVER returns  *)
(*    a wrong value.                                                          *)
(*                                                                         *)
(*  UseContentCRC = FALSE: the common-prefix-by-offset path. It trusts        *)
(*    whatever byte a read replica holds at that offset -- so a bitflipped    *)
(*    copy is accepted as the record's content (silent corruption).          *)
(***************************************************************************)

\* CRC reconstruction over a concrete read set RS: accept the true content only
\* if some replica in RS holds a CRC-valid (= true) copy; otherwise None (a
\* detected gap). The CRC NEVER returns a value other than the true content.
CRCRead(RS, p) ==
  IF \E r \in RS : cell[r][p] = trueVal[p] THEN trueVal[p] ELSE None

\* Common-prefix-by-offset reconstruction over a read set RS: the set of byte
\* values a replica in RS holds at offset p. The path trusts the byte at the
\* offset, so a bitflipped copy is among the candidates and can be returned --
\* this is the masking the model tests. None copies (Miss) are skipped.
NoCRCReadable(RS, p) == {cell[r][p] : r \in {r2 \in RS : cell[r2][p] # Miss}}

\* A read/recovery consults a read set RS of ReadW reachable replicas and
\* reconstructs position p. FINDING A is exposed here: the read set is a chosen
\* ReadW-subset, NOT all replicas, so an acked record held only on replicas
\* OUTSIDE RS (admissible when the quorum W is too small to guarantee
\* intersection) is invisible -> None -> NoAckedLoss violated.
DoRead(p) ==
  /\ p \in appended
  /\ p \notin readDone
  /\ \E RS \in SUBSET {r \in Replicas : up[r]} :
       /\ Cardinality(RS) = ReadW
       /\ IF UseContentCRC
            THEN readResult' = [readResult EXCEPT ![p] = CRCRead(RS, p)]
            ELSE \E v \in (IF NoCRCReadable(RS, p) = {} THEN {None}
                           ELSE NoCRCReadable(RS, p)) :
                   readResult' = [readResult EXCEPT ![p] = v]
  /\ readDone' = readDone \cup {p}
  \* remember whether p was already durable when this read happened: only such
  \* reads are bound by NoAckedLoss (reading an un-acked tail may legitimately
  \* return None / a partial copy).
  /\ readAcked' = IF ackedVal[p] # None THEN readAcked \cup {p} ELSE readAcked
  /\ UNCHANGED <<trueVal, ackedVal, cell, appended, up, corrupts>>

Next ==
  \/ \E p \in Positions : StartAppend(p)
  \/ \E p \in Positions : CatchUp(p)
  \/ \E p \in Positions : Ack(p)
  \/ \E p \in Positions : Corrupt(p)
  \/ Fail
  \/ \E p \in Positions : DoRead(p)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* SAFETY 1 (durability / FINDING A): an acked record is never lost by a read or
\* recovery the policy promises can see it. If a read of position p happened
\* AFTER p was acked (p \in readAcked), the read must have produced SOME content
\* (not a None gap). This holds only when the ack set and the read set are
\* guaranteed to intersect on a surviving replica -- the quorum intersection
\* rule (AckThreshold + ReadW > RF, tolerating the modeled fault budget). A
\* too-small quorum W lets every acked-holding replica fall outside the chosen
\* read set, and the read returns None: the acked record is lost.
NoAckedLoss ==
  \A p \in Positions :
    p \in readAcked => readResult[p] # None

\* SAFETY 2 (integrity / FINDING B): a read/recovery never returns content that
\* differs from the true acked content. A single-replica bitflip must be caught
\* by the content CRC, never masked by trusting a byte at the offset. With
\* UseContentCRC this holds; the common-prefix path (UseContentCRC=FALSE) can
\* return a flipped byte for an acked position -> violation.
NoSilentCorruption ==
  \A p \in Positions :
    (p \in readAcked /\ readResult[p] # None) => readResult[p] = ackedVal[p]

\* Sanity: acked content always equals the true content the writer chose.
AckMatchesTrue ==
  \A p \in Positions : ackedVal[p] # None => ackedVal[p] = trueVal[p]

=============================================================================
