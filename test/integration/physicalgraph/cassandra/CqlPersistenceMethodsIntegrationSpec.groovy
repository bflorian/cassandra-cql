package physicalgraph.cassandra

import com.datastax.driver.core.utils.UUIDs
import physicalgraph.cassandra.util.TestSchema

class CqlPersistenceMethodsIntegrationSpec extends CassandraIntegrationSpec
{
	//def cqlService

	def "columnFamily"() {
		given:
		def session = cqlService.keyspace()

		when:
		def cf = mapping.columnFamily(session, "Event")
		println cf

		then:
		cf.name == "Event"
		cf.primaryKey[0].name == "key"
	}

	def "keyType"() {
		given:
		def session = cqlService.keyspace()

		expect:
		keyType == mapping.keyType(session, table)

		where:
		table 			| keyType
		"Event" 		| "timeuuid"
		"Event_IDX" 	| "text"
		"Event_IDX2" 	| "text"
		"DeviceState" 	| "uuid"
	}

	def "indexIsReversed"() {
		given:
			def session = cqlService.keyspace()

		expect:
			isReversed == mapping.indexIsReversed(session, table)
			isTimeUuid == mapping.indexIsTimeUuid(mapping.columnFamily(session, table))

		where:
			table 			| isReversed	| isTimeUuid
			"Event" 		| false         | true
			"Event_IDX" 	| true 			| true
			"Event_IDX2" 	| true			| true
			"DeviceState" 	| false         | false

	}

	def "putColumn name"() {
		given:
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuid = UUID.fromString("25847f3c-3444-11e4-9de7-22000b420e4e")

		when:
		mapping.putColumn(mb, cf, uuid, "name", "switch")
		mapping.execute(mb)

		then:
		stringValue("Event", uuid, "name") == "switch"
	}

	def "putColumn value=on"() {
		given:
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuid = UUID.fromString("25847f3c-3444-11e4-9de7-22000b420e4e")

		when:
		mapping.putColumn(mb, cf, uuid, "value", "on")
		mapping.execute(mb)

		then:
		stringValue("Event", uuid, "value") == "on"
	}

	def "putColumn value=off"() {
		given:
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuid = UUID.fromString("25847f3c-3444-11e4-9de7-22000b420e4e")

		when:
		mapping.putColumn(mb, cf, uuid, "value", "off")
		mapping.execute(mb)

		then:
		stringValue("Event", uuid, "value") == "off"
	}

	def "putColumn with TTL"() {
		given:
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuid = UUID.fromString("2ea53650-342c-11e4-b4d6-22000b0e109f")

		when:
		mapping.putColumn(mb, cf, uuid, "name", "switch", 120)
		mapping.execute(mb)

		then:
		stringValue("Event", uuid, "name") == "switch"
	}

	def "putColumns"() {
		given:
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuid = UUID.fromString("756f6772-342c-11e4-b208-22000b468786")

		when:
		mapping.putColumns(mb, cf, uuid, [name: "contact", value: "closed", description: "The front door is closed", viewed: "true", date:"2014-09-08T00:00:00.000Z"])
		mapping.execute(mb)

		then:
		stringValue("Event", uuid, "name") == "contact"
		stringValue("Event", uuid, "value") == "closed"
		stringValue("Event", uuid, "description") == "The front door is closed"
		stringValue("Event", uuid, "viewed") == "true"
		stringValue("Event", uuid, "date") == "2014-09-08T00:00:00.000Z"
	}

	def "deleteColumn"() {
		given:
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuid = UUID.fromString("6f2e2a68-3429-11e4-b208-22000b468786")
		mapping.putColumns(mb, cf, uuid, [name: "contact", value: "closed", description: "The front door is closed", viewed: "true", date:"2014-09-08T00:00:00.000Z"])

		when:
		mapping.deleteColumn(mb, cf, uuid, "viewed")
		mapping.execute(mb)

		then:
		stringValue("Event", uuid, "name") == "contact"
		stringValue("Event", uuid, "value") == "closed"
		stringValue("Event", uuid, "description") == "The front door is closed"
		stringValue("Event", uuid, "viewed") == null
		stringValue("Event", uuid, "date") == "2014-09-08T00:00:00.000Z"

	}

	def "putColumn null"() {
		given:
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuid = UUID.fromString("756d3786-342c-11e4-b208-22000b468786")
		mapping.putColumns(mb, cf, uuid, [name: "contact", value: "closed", description: "The front door is closed", viewed: "true", date:"2014-09-08T00:00:00.000Z"])

		when:
		mapping.putColumn(mb, cf, uuid, "date", null)
		mapping.execute(mb)

		then:
		stringValue("Event", uuid, "name") == "contact"
		stringValue("Event", uuid, "value") == "closed"
		stringValue("Event", uuid, "description") == "The front door is closed"
		stringValue("Event", uuid, "viewed") == "true"
		stringValue("Event", uuid, "date") == null
	}

	def "getRow"() {
		given:
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuid = UUIDs.timeBased()
		mapping.putColumns(mb, cf, uuid, [name: "contact", value: "closed", description: "The front door is closed"])
		mapping.execute(mb)

		when:
		def row = mapping.getRow(session, cf, uuid, null)

		then:
		mapping.getColumn(row,"name").value == "contact"
		mapping.stringValue(mapping.getColumn(row,"value")) == "closed"
		mapping.getColumn(row,"description").value == "The front door is closed"
	}

	def "getRows"() {
		given:
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuid1 = UUIDs.timeBased()
		mapping.putColumns(mb, cf, uuid1, [name: "contact", value: "closed", description: "The front door is closed"])
		def uuid2 = UUIDs.timeBased()
		mapping.putColumns(mb, cf, uuid2, [name: "switch", value: "on", description: "The porch light is on"])
		mapping.execute(mb)

		when:
		def rows = mapping.getRows(session, cf, [uuid1,uuid2], null)

		then:
		mapping.getColumn(rows[uuid1], "name").value == "contact"
		mapping.getColumn(rows[uuid2], "value").value == "on"
		mapping.getColumn(rows[uuid2], "description").value == "The porch light is on"
	}

	def "getRowsColumnSlice"() {
		given:
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuid1 = UUIDs.timeBased()
		mapping.putColumns(mb, cf, uuid1, [name: "contact", value: "closed", description: "The front door is closed"])
		def uuid2 = UUIDs.timeBased()
		mapping.putColumns(mb, cf, uuid2, [name: "switch", value: "on", description: "The porch light is on"])
		mapping.execute(mb)

		when:
		def rows = mapping.getRowsColumnSlice(session, cf, [uuid1,uuid2], ["name","value"],  null)

		then:
		mapping.getColumn(rows[uuid1], "name").value == "contact"
		mapping.getColumn(rows[uuid2], "value").value == "on"

		when:
		mapping.getColumn(rows[uuid2], "description")

		then:
		IllegalArgumentException e = thrown()
	}

	def "getRowsColumnRange asc"() {
		given:
		def rowKey = "getRowsColumnRange-test"
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event_IDX2")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuids = []
		5.times {
			uuids << UUIDs.timeBased()
			Thread.sleep(10)
		}
		def i = 0
		mapping.putColumns(mb, cf, rowKey, uuids.collectEntries{[it,"${++i}".toString()]})
		mapping.execute(mb)

		when:
		// TODO - specify these reversed since the CF is reversed. Is that right?
		def rows = mapping.getRowsColumnRange(session, cf, [rowKey], uuids[1], uuids[3], false, 10, null)

		then:
		rows.size() == 1
	}

	def "getRowsColumnRange desc"() {
		given:
		def rowKey = "getRowsColumnRange-test"
		def session = cqlService.keyspace()
		def cf = mapping.columnFamily(session, "Event_IDX")
		def mb = mapping.prepareMutationBatch(session, "ONE")
		def uuids = []
		5.times {
			uuids << UUIDs.timeBased()
			Thread.sleep(10)
		}
		def i = 0
		mapping.putColumns(mb, cf, rowKey, uuids.collectEntries{[it,"${++i}".toString()]})
		mapping.execute(mb)

		when:
		// TODO - specify these reversed since the CF is reversed. Is that right?
		def rows = mapping.getRowsColumnRange(session, cf, [rowKey], uuids[3], uuids[1], false, 10, null)

		then:
		rows.size() == 1
	}

	private getMapping()
	{
		cqlService.orm
	}
}
