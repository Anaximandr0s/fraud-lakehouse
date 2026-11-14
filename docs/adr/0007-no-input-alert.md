# 0007 — "No streaming input" alert gated by the producer

**Context.** §10 step 7 asks for an email within 6 min of the producer stopping. The obvious rule, "Spark input
rate = 0 for 5 min", also fires when everything is fine: the producer replays the CSV once, then stays idle, and the
alert would ring forever after a complete replay.

**Decision.** The producer exposes `producer_rows_sent_total` and `producer_finished` (0 while replaying, 1 once the
end of the CSV is reached), port 8000. The rule is `input rate == 0 unless producer_finished == 1`, for 4 min:
silent after a complete replay, and firing when the producer is stuck or gone (its series go stale). 4 min, not 5:
Spark's next empty batch, the 15 s scrape, the 30 s evaluation and a 10 s group wait add about 1.5 min, so the email
leaves about 5 min after the stop (measured 5 min 25 s). The memory alert divides the cAdvisor working set, not
usage, by the limit: usage counts page cache the kernel can drop.

**Consequences.** Stopping the producer sends two emails: Target down (about 1.5 min) and No streaming input
(about 5.5 min). The margin under 6 min is about 35 s.
