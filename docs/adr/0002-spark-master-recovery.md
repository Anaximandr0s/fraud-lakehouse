# 0002 — Spark master recovery on a filesystem volume

**Context.** In Spark Standalone without recovery, a restarted `spark-master` forgets running applications. The worker
re-registers, but the Thrift driver logs "waiting for master to reconnect" forever and queries hang (tested: a Beeline
query timed out after 90 s). Its healthcheck stays green, and Docker never restarts a container for being unhealthy.

**Decision.** `spark.deploy.recoveryMode=FILESYSTEM` on the master, with its state in the named volume `spark-recovery`
(`/opt/spark/recovery`, created in the image so the spark user owns it). ZooKeeper mode was not chosen: it needs an
extra service for a single master.

**Consequences.** After `docker compose restart spark-master`, the Thrift app is recovered and Beeline answers again.
The same covers spark-streaming (step 4) and Jupyter. Deleting the volume only loses the master's view of running apps.
