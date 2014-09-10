package physicalgraph.cassandra

import com.datastax.driver.core.querybuilder.QueryBuilder
import grails.plugin.spock.IntegrationSpec
import physicalgraph.cassandra.util.TestSchema

abstract class CassandraIntegrationSpec extends grails.plugin.spock.IntegrationSpec
{
	def cqlService

	def setup() {
		new TestSchema().initialize(cqlService)
	}

	def column(table, rowKey, columnName) {
		def statement = QueryBuilder.select()
			.column(columnName)
			.from("\"$table\"")
			.where(QueryBuilder.eq("key", rowKey))

		cqlService.keyspace().execute(statement).one()
	}

	def stringValue(table, rowKey, columnName) {
		column(table, rowKey, columnName).getString(0)
	}

	def uuidValue(table, rowKey, columnName) {
		column(table, rowKey, columnName).getUUID(0)
	}
}
