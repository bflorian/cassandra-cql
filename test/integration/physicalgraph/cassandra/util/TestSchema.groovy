package physicalgraph.cassandra.util

class TestSchema
{
	static initialized = false

	void initialize(cqlService) {
		if (!initialized) {
			dropSchema(cqlService)
			createSchema(cqlService)
			initialized = true
		}
	}

	void dropSchema(cqlService) {
		try {
			cqlService.execute("DROP KEYSPACE cqltest")
		}
		catch (Exception e) {
			println "$e"
		}
	}

	void createSchema(cqlService) {
		cqlService.execute("CREATE KEYSPACE cqltest WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")

		cqlService.keyspace().execute(EVENT)
		cqlService.keyspace().execute(EVENT_IDX)
		cqlService.keyspace().execute(EVENT_IDX2)
		//cqlService.keyspace().execute(EVENT_LNK)
		cqlService.keyspace().execute(EVENT_CTR)
		cqlService.keyspace().execute(DEVICE_STATE)
	}

	static final String EVENT =
"""
CREATE TABLE "Event" (
  key timeuuid,
  date text,
  description text,
  name text,
  value text,
  viewed text,
  PRIMARY KEY (key)
);
"""
	static final String EVENT_IDX =
"""
CREATE TABLE "Event_IDX" (
  key text,
  column1 timeuuid,
  value text,
  PRIMARY KEY (key, column1)
) WITH CLUSTERING ORDER BY (column1 DESC);"""

	static final String EVENT_IDX2 =
		"""
CREATE TABLE "Event_IDX2" (
  key text,
  column1 timeuuid,
  value text,
  PRIMARY KEY (key, column1)
);"""

	static final String EVENT_CTR =
"""
CREATE TABLE "Event_CTR" (
  key text,
  column1 text,
  value counter,
  PRIMARY KEY (key, column1)
);
"""

	static final String DEVICE_STATE =
		"""
CREATE TABLE "DeviceState" (
  key uuid,
  date text,
  name text,
  value text,
  PRIMARY KEY (key)
);
"""
}
