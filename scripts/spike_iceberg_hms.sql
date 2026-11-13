DROP TABLE IF EXISTS lakehouse.spike.t PURGE;
DROP NAMESPACE IF EXISTS lakehouse.spike;
CREATE NAMESPACE lakehouse.spike LOCATION 's3a://lakehouse/warehouse/spike.db';
CREATE TABLE lakehouse.spike.t (id INT, label STRING) USING iceberg;
INSERT INTO lakehouse.spike.t VALUES (1, 'a'), (2, 'b');
SELECT * FROM lakehouse.spike.t ORDER BY id;
SELECT file_path FROM lakehouse.spike.t.files;
