# 0001 — Iceberg catalog: Hive Metastore 3.1.3

**Context.** Step 1 spike (`make spike`): from a throwaway Spark 3.5.9 container with Iceberg 1.12.0, create a
namespace at `s3a://lakehouse/warehouse/spike.db`, an Iceberg table, insert and select, through the Hive Metastore.
Hive Metastore 4.2.1 and 4.0.1 both fail with `Invalid method name: 'get_table'`: Hive 4 removed that Thrift call,
and Spark 3.5's built-in Hive client (2.3.9, used by Iceberg's `HiveCatalog`) supports metastores up to 3.1.3 only.

**Decision.** Keep the Hive Metastore as catalog, pinned to `apache/hive:3.1.3`. The spike passes on it: rows read
back, Parquet and metadata files written to MinIO, catalog intact after `make down && make up-core`.
The image ships hadoop-aws 3.1.0 and its AWS SDK (enabled with `HADOOP_OPTIONAL_TOOLS`); the pinned Postgres JDBC
driver is mounted over its 2016 driver, which cannot do SCRAM-SHA-256. Its entrypoint's `-initSchema` is not
re-runnable, so a one-shot `hive-schema-init` service creates the schema only when none exists.

**Consequences.** The metastore runs on Java 8 and an old Hadoop; it only stores metadata, Spark does the data I/O.
Moving to Hive 4 waits for Spark 4. The JDBC catalog fallback was not needed.
