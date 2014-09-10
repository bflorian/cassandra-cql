package physicalgraph.cassandra

import com.datastax.driver.core.Row
import com.datastax.driver.core.TableMetadata

class CqlRow
{
	Row row
	TableMetadata columnFamily

	def getColumns() {
		def keyName = rowKeyName(columnFamily)
		row.columnDefinitions.findAll { it.name != keyName }.collect{new CqlColumn(row: row, name: it.name, type: it.type)}
	}

	static String rowKeyName(columnFamily) {
		columnFamily.primaryKey[0].name
	}

}
