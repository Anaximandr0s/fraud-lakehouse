# 0004 — Streaming: one foreachBatch for Bronze and Silver

**Context.** Spark Structured Streaming must write Kafka events to Bronze (raw) and Silver (one row per
`trans_num`) with no loss and no duplicates in Silver, through restarts and crashes.

**Decision.** One query, one `foreachBatch` every 30 s: append the batch to Bronze, append rejected rows to
`silver.transactions_rejected`, then `MERGE INTO silver.transactions … WHEN NOT MATCHED THEN INSERT *` on `trans_num`.
Insert-only: a replayed event is identical to the stored one, so the merge writes no delete files.
The checkpoint lives on a local named volume, not on `s3a://`: Spark commits checkpoint files by rename, which
S3A does not do atomically, and the query keeps no state, so only the driver writes it.
`maxOffsetsPerTrigger` caps a batch; `failOnDataLoss=true` stops the query if Kafka deleted unread events.
Rejected: two queries (Kafka → Bronze, Bronze → Silver), which double the checkpoints and the latency.

**Consequences.** If a batch fails after its Bronze or rejected append, the replay appends those rows again:
Bronze and the rejected table may hold duplicates; Silver stays unique. A failed query ends the process and
Docker restarts the container, which resumes from the checkpoint.
