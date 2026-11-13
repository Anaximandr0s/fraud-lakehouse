# 0003 — Producer resume: saved row position, at-least-once

**Context.** The producer must survive restarts without losing events. Kafka does not track a producer's progress,
and Spark (step 4) does not commit consumer offsets either, so the producer keeps its own position.

**Decision.** The producer stores the number of CSV rows already sent in `/state/position` (named volume
`producer-state`), written atomically (temp file + rename) every second, only after `flush()` confirms Kafka
acknowledged every earlier event. On SIGTERM it flushes and saves before exiting. After a failed delivery it
exits without saving. At the end of the file it stays idle and keeps touching a heartbeat file that the
healthcheck reads. Rejected: Kafka transactions (exactly-once), which would need a transactional id, fencing and
`read_committed` consumers for a single-writer replay.

**Consequences.** At-least-once: a crash between a send and the next save resends up to about 1 s of events.
Silver removes them with `MERGE` on `trans_num` (step 4). A stop, a restart and recreating the container all
matched the Kafka offsets exactly in step 3 tests. `make reset-producer` replays from row 0 and leaves the
earlier events in the topic.
