package physicalgraph.cassandra

import com.datastax.driver.core.ColumnDefinitions
import com.datastax.driver.core.DataType
import com.datastax.driver.core.Row

class CqlColumn
{
	Row row
	DataType type
	String name

	def getValue() {
		columnValue(row, name, type)
	}

	String getStringValue() {
		row.getString(name)
	}

	Long getLongValue() {
		row.getLong(name)
	}

	UUID getUUIDValue() {
		row.getUUID(name)
	}

	byte[] getByteArrayValue() {
		row.getBytes(name).array()
	}

	static columnValue(row, name, type) {
		if (type == DataType.uuid() || type == DataType.timeuuid()) {
			row.getUUID(name)
		}
		else {
			row.getString(name)
		}
	}
}
