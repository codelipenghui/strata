-------------------------- MODULE MetadataManifestCAS --------------------------
(***************************************************************************)
(* Models manifest publication for one namespace the way the code actually   *)
(* does it: a compare-and-set on the ZooKeeper znode VERSION, with            *)
(* metadataEpoch as a secondary monotonic fence (sections 9, 15 of            *)
(* strata-metadata-scaling-design.md; ZkMetadataStore.putNamespaceManifest +  *)
(* NamespaceMetadataLogRepository.requirePublishEpoch).                       *)
(*                                                                         *)
(* This isolates the claim the review reached after reading the code: the     *)
(* linearizable single-writer barrier for manifest publication is the         *)
(* znode-version CAS, NOT the epoch. The epoch (requirePublishEpoch rejects    *)
(* a strictly-lower epoch) is a secondary stale-term fence. Two toggles let    *)
(* us check each guard in isolation:                                          *)
(*   UseVersionCAS  -- publish requires expectedVersion = current znode ver.   *)
(*   UseEpochFence  -- publish requires myEpoch >= published epoch.            *)
(*                                                                         *)
(* Expected results (see the .cfg and the design doc section 18):             *)
(*   both TRUE  (faithful to code):    NoPublishedRegression holds.            *)
(*   version only (epoch fence off):   holds  -> epoch is NOT needed for it.   *)
(*   epoch only  (version CAS off):    FAILS  -> a stale-read leader, even     *)
(*                                     with a HIGHER epoch, clobbers a newer    *)
(*                                     manifest and regresses the published     *)
(*                                     offset. Version CAS is the real barrier. *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS Leaders, MaxEpoch, MaxOffset, MaxVersion, UseVersionCAS, UseEpochFence

VARIABLES
  mfVersion,          \* manifest znode version (bumped on each successful CAS)
  mfEpoch,            \* metadataEpoch recorded in the published manifest
  mfOffset,           \* publishedLogOffset recorded in the published manifest
  epochCounter,       \* /strata/meta/epoch counter (source of new epochs)
  lEpoch,             \* [Leaders -> Nat] epoch each leader holds
  lReadVersion,       \* [Leaders -> Nat] manifest version each leader last read
  lOffset,            \* [Leaders -> Nat] offset each leader intends to publish
  maxPublishedOffset  \* aux/history: highest offset ever published

vars == <<mfVersion, mfEpoch, mfOffset, epochCounter,
          lEpoch, lReadVersion, lOffset, maxPublishedOffset>>

TypeOK ==
  /\ mfVersion \in 0..MaxVersion
  /\ mfEpoch \in 0..MaxEpoch
  /\ mfOffset \in 0..MaxOffset
  /\ epochCounter \in 0..MaxEpoch
  /\ lEpoch \in [Leaders -> 0..MaxEpoch]
  /\ lReadVersion \in [Leaders -> 0..MaxVersion]
  /\ lOffset \in [Leaders -> 0..MaxOffset]
  /\ maxPublishedOffset \in 0..MaxOffset

Init ==
  /\ mfVersion = 0
  /\ mfEpoch = 0
  /\ mfOffset = 0
  /\ epochCounter = 0
  /\ lEpoch = [l \in Leaders |-> 0]
  /\ lReadVersion = [l \in Leaders |-> 0]
  /\ lOffset = [l \in Leaders |-> 0]
  /\ maxPublishedOffset = 0

\* Win a new term: CAS-increment the global epoch counter (strictly greater).
AcquireEpoch(l) ==
  /\ epochCounter < MaxEpoch
  /\ epochCounter' = epochCounter + 1
  /\ lEpoch' = [lEpoch EXCEPT ![l] = epochCounter + 1]
  /\ UNCHANGED <<mfVersion, mfEpoch, mfOffset, lReadVersion, lOffset, maxPublishedOffset>>

\* Read the current manifest: remember its version (the expectedVersion for a
\* later CAS) and adopt its published offset as the recovery floor.
ReadManifest(l) ==
  /\ lReadVersion' = [lReadVersion EXCEPT ![l] = mfVersion]
  /\ lOffset' = [lOffset EXCEPT ![l] = mfOffset]
  /\ UNCHANGED <<mfVersion, mfEpoch, mfOffset, epochCounter, lEpoch, maxPublishedOffset>>

\* Local progress: the leader appends/compacts and wants to publish a higher
\* offset than it last read. (A stale leader that never re-reads keeps a low
\* read version while its offset may lag the truly-published offset.)
Advance(l) ==
  /\ lOffset[l] < MaxOffset
  /\ lOffset' = [lOffset EXCEPT ![l] = lOffset[l] + 1]
  /\ UNCHANGED <<mfVersion, mfEpoch, mfOffset, epochCounter, lEpoch, lReadVersion, maxPublishedOffset>>

\* Publish a new manifest. The guards are exactly the two mechanisms under test.
PublishCAS(l) ==
  /\ mfVersion < MaxVersion
  /\ lEpoch[l] >= 1
  /\ (UseVersionCAS => lReadVersion[l] = mfVersion)   \* the znode-version CAS
  /\ (UseEpochFence  => lEpoch[l] >= mfEpoch)          \* requirePublishEpoch
  /\ mfVersion' = mfVersion + 1
  /\ mfEpoch' = lEpoch[l]
  /\ mfOffset' = lOffset[l]
  /\ lReadVersion' = [lReadVersion EXCEPT ![l] = mfVersion + 1]
  /\ maxPublishedOffset' = IF lOffset[l] > maxPublishedOffset THEN lOffset[l] ELSE maxPublishedOffset
  /\ UNCHANGED <<epochCounter, lEpoch, lOffset>>

Next ==
  \E l \in Leaders :
    \/ AcquireEpoch(l)
    \/ ReadManifest(l)
    \/ Advance(l)
    \/ PublishCAS(l)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* SAFETY: the published log offset never regresses -- no leader's publish ever
\* overwrites a newer manifest with older/less-advanced state. This is the
\* observable harm a stale clobber would cause (published metadata lost). The
\* version CAS guarantees it: a successful CAS proves the publisher read the
\* current manifest, so its offset is >= the published offset.
NoPublishedRegression ==
  mfOffset = maxPublishedOffset

\* Sanity: a published manifest's epoch never exceeds the consensus counter.
PublishedEpochBounded ==
  mfEpoch <= epochCounter

=============================================================================
