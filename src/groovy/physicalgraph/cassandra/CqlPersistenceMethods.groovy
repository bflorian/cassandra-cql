package physicalgraph.cassandra

import com.datastax.driver.core.DataType
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Statement
import com.datastax.driver.core.TableMetadata
import com.datastax.driver.core.querybuilder.QueryBuilder
import static com.datastax.driver.core.querybuilder.QueryBuilder.*
import com.datastax.driver.core.querybuilder.Using
import com.reachlocal.grails.plugins.cassandra.mapping.PersistenceProvider

import static com.datastax.driver.core.querybuilder.QueryBuilder.incr

class CqlPersistenceMethods //implements PersistenceProvider
{
	def log

	private logTime(long t0, name) {
		log.trace "CQL.$name ${System.currentTimeMillis() - t0} msec"
	}

	// Read operations
	def columnTypes(Object client, String name)
	{
		long t0 = System.currentTimeMillis()
		def result = [:]
		def cf = columnFamily(client, name)
		cf.columns.each {
			result[it.name] = it.type.name
		}
		logTime(t0, "columnTypes")
		result
	}

	def keyType(Object client, String name)
	{
		long t0 = System.currentTimeMillis()
		def result = rowKeyType(columnFamily(client, name))
		logTime(t0, "keyType")
		result
	}

	def columnFamily(Object client, String name)
	{
		long t0 = System.currentTimeMillis()
		def result = client.cluster.metadata.getKeyspace(client.loggedKeyspace).getTable(name)
		logTime(t0, "columnFamily")
		result
	}

	static indexIsTimeUuid(indexColumnFamily) {
		columnKeyType(indexColumnFamily) == "timeuuid"
	}

	static keyIsTimeUuid(columnFamily) {
		rowKeyType(columnFamily) == "timeuuid"
	}

	def indexIsReversed(Object client, String indexColumnFamilyName) {
		long t0 = System.currentTimeMillis()

		def result = false
		def orders = columnFamily(client, indexColumnFamilyName).clusteringOrder
		if (orders) {
			def values = orders[0]?.values()
			if (values) {
				result = values[-1] == TableMetadata.Order.DESC
			}
		}
		logTime(t0, "indexIsReversed")
		result
	}

	def columnFamilyName(columnFamily)
	{
		columnFamily.name
	}

	def getRow(Object client, Object columnFamily, Object rowKey, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()

		def statement = QueryBuilder.select()
			.all()
			.from(tableName(columnFamily))
			.where(eq(rowKeyName(columnFamily), rowKey))

		def result = new CqlRow(row: client.execute(statement).one(), columnFamily: columnFamily)

		logTime(t0, "getRow")
		result
	}

	def getRows(Object client, Object columnFamily, Collection rowKeys, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()

		def statement = QueryBuilder.select()
			.all()
			.from(tableName(columnFamily))
			.where(QueryBuilder.in(rowKeyName(columnFamily), *rowKeys))

		def result = rows(client.execute(statement), columnFamily)

		logTime(t0, "getRows")
		result
	}

	def getRowsColumnSlice(Object client, Object columnFamily, Collection rowKeys, Collection columnNames, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()

		def statement = QueryBuilder.select(*([rowKeyName(columnFamily)] + columnNames))
			.from(tableName(columnFamily))
			.where(QueryBuilder.in(rowKeyName(columnFamily), *rowKeys))

		def result = rows(client.execute(statement), columnFamily)

		logTime(t0, "getRowsColumnSlice")
		result
	}


	def getRowsColumnRange(Object client, Object columnFamily, Collection rowKeys, Object start, Object finish, Boolean reversed, Integer max, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()

		def statement = QueryBuilder.select()
			.from(tableName(columnFamily))
			.where(QueryBuilder.in(rowKeyName(columnFamily), *rowKeys))
			.and(lte(columnKeyName(columnFamily), start))
			.and(gte(columnKeyName(columnFamily), finish))

		def result = rows(client.execute(statement), columnFamily)

		logTime(t0, "getRowsColumnRange")
		result
	}
/*
	def getRowsWithEqualityIndex(client, columnFamily, properties, max, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def exp = properties.collect {name, value ->
			columnFamily.newIndexClause().whereColumn(name).equals().value(value)
		}

		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
			.searchWithIndex()
			.setRowLimit(max)
			.addPreparedExpressions(exp)
			.execute()
			.result
		logTime(t0, "getRowsWithEqualityIndex")
		result
	}

	def countRowsWithEqualityIndex(client, columnFamily, properties, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def clause = properties.collect {name, value -> "${name} = '${value}'"}.join(" AND ")
		def query = "SELECT COUNT(*) FROM ${columnFamily.name} WHERE ${clause}"

		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
			.withCql(query)
			.execute()
			.result
			.rows
			.getRowByIndex(0)
			.columns
			.getColumnByIndex(0)
			.longValue
		logTime(t0, "countRowsWithEqualityIndex")
		result
	}

	def getRowsWithCqlWhereClause(client, columnFamily, clause, max, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def query = "SELECT * FROM ${columnFamily.name} WHERE ${clause} LIMIT ${max}"

		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
			.withCql(query)
			.execute()
			.result
		logTime(t0, "getRowsWithCqlWhereClause")
		result
	}

	def getRowsColumnSliceWithCqlWhereClause(client, columnFamily, clause, max, columns, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def query = "SELECT ${columns.join(', ')} FROM ${columnFamily.name} WHERE ${clause} LIMIT ${max}"

		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
			.withCql(query)
			.execute()
			.result
		logTime(t0, "getRowsColumnSliceWithCqlWhereClause")
		result
	}

	def countRowsWithCqlWhereClause(client, columnFamily, clause, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def query = "SELECT COUNT(*) FROM ${columnFamily.name} WHERE ${clause}"

		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
			.withCql(query)
			.execute()
			.result
			.rows
			.getRowByIndex(0)
			.columns
			.getColumnByIndex(0)
			.longValue
		logTime(t0, "countRowsWithCqlWhereClause")
		result
	}

	def getColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Boolean reversed, Integer max, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey)
			.withColumnRange(start, finish, reversed, max)
			.execute()
			.result
		logTime(t0, "getColumnRange")
		result
	}

	def countColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def row = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey)
		if (start || finish) {
			row = row.withColumnRange(start, finish, false, Integer.MAX_VALUE)
		}
		def result = row.getCount()
			.execute()
			.result
		logTime(t0, "countColumnRange")
		result
	}

	def getColumnSlice(Object client, Object columnFamily, Object rowKey, Collection columnNames, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey)
			.withColumnSlice(columnNames)
			.execute()
			.result
		logTime(t0, "getColumnSlice")
		result
	}

	def getColumn(Object client, Object columnFamily, Object rowKey, Object columnName, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey)
			.getColumn(columnName)
			.execute()
			.result
		logTime(t0, "getColumn")
		result
	}

	def prepareMutationBatch(client, ConsistencyLevel consistencyLevel)
	{
		def m = client.prepareMutationBatch()
		if (consistencyLevel) {
			m.setConsistencyLevel(consistencyLevel)
		}
		return m
	}
*/
	def prepareMutationBatch(client, consistencyLevel)
	{
		new CqlMutationBatch(session: client, consistencyLevel: consistencyLevel)
	}

	void deleteColumn(mutationBatch, columnFamily, rowKey, columnName)
	{
		def statement = QueryBuilder.delete(columnName)
			.from(tableName(columnFamily))
			.where(eq(rowKeyName(columnFamily), rowKey))

		mutationBatch.statements << statement
	}

	void putColumn(mutationBatch, columnFamily, rowKey, name, value)
	{
		def statement = QueryBuilder.insertInto(tableName(columnFamily))
			.value(rowKeyName(columnFamily), rowKey)

		if (name instanceof String && columnFamily.getColumn(name)) {
			statement.value(name, value)
		}
		else {
			statement.value(columnKeyName(columnFamily), name)
			statement.value(columnValueName(columnFamily), value)
		}

		mutationBatch.statements << statement
	}

	void putColumn(mutationBatch, columnFamily, rowKey, name, value, ttl)
	{
		def statement = QueryBuilder.insertInto(tableName(columnFamily))
			.value(rowKeyName(columnFamily), rowKey)

		if (name instanceof String && columnFamily.getColumn(name)) {
			statement.value(name, value)
		}
		else {
			statement.value(columnKeyName(columnFamily), name)
			statement.value(columnValueName(columnFamily), value)
		}

		if (ttl) {
			statement.using(QueryBuilder.ttl(ttl as int))
		}

		mutationBatch.statements << statement
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap)
	{
		def aKey = columnMap.keySet().toList()[0]
		if (aKey instanceof String && columnFamily.getColumn(aKey)) {
			def statement = QueryBuilder.insertInto(tableName(columnFamily)).value(rowKeyName(columnFamily), rowKey)
			columnMap.each { k, v ->
				statement.value(k, v)
			}
			mutationBatch.statements << statement
		}
		else {
			def table = tableName(columnFamily)
			def row = rowKeyName(columnFamily)
			columnMap.each { k, v ->
				def statement = QueryBuilder.insertInto(table)
					.value(row, rowKey)
					.value(columnKeyName(columnFamily), k)
					.value(columnValueName(columnFamily), v)

				mutationBatch.statements << statement
			}
		}
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap, ttlMap)
	{
		def aKey = columnMap.keySet().toList()[0]
		if (ttlMap instanceof Number) {
			if (aKey instanceof String && columnFamily.getColumn(aKey)) {
				def statement = QueryBuilder.insertInto(tableName(columnFamily)).value(rowKeyName(columnFamily), rowKey)
				columnMap.each { k, v ->
					statement.value(k, v)
				}
				statement.using(ttl(ttlMap as int))
				mutationBatch.statements << statement
			}
			else {
				def table = tableName(columnFamily)
				def row = rowKeyName(columnFamily)
				columnMap.each { k, v ->
					def statement = QueryBuilder.insertInto(table)
						.value(row, rowKey)
						.value(columnKeyName(columnFamily), k)
						.value(columnValueName(columnFamily), v)

					statement.using(ttl(ttlMap as int))
					mutationBatch.statements << statement
				}
			}
		}
		else {
			def table = tableName(columnFamily)
			def row = rowKeyName(columnFamily)
			columnMap.each { k, v ->
				def statement = QueryBuilder.insertInto(table)
					.value(row, rowKey)
					.value(columnKeyName(columnFamily), k)
					.value(columnValueName(columnFamily), v)

				def ttlValue = ttlMap[k]
				if (ttlValue) {
					statement.using(ttl(ttlValue as int))
				}
				mutationBatch.statements << statement
			}
		}
	}

	void incrementCounterColumn(mutationBatch, columnFamily, rowKey, columnName)
	{
		//mutationBatch.withRow(columnFamily, rowKey).incrementCounterColumn(columnName, 1)
		def statement = QueryBuilder.insertInto(tableName(columnFamily))
			.with(columnValueName(columnFamily))
			.where(eq(rowKeyName(columnFamily), rowKey))
			.and(eq(columnName(columnFamily), columnName))

		mutationBatch.statements << statement
	}

	void incrementCounterColumn(mutationBatch, columnFamily, rowKey, columnName, Long value)
	{
		//mutationBatch.withRow(columnFamily, rowKey).incrementCounterColumn(columnName, value)
		def statement = QueryBuilder.insertInto(tableName(columnFamily))
			.with(incr(columnValueName(columnFamily), value))
			.where(eq(rowKeyName(columnFamily), rowKey))
			.and(eq(columnKeyName(columnFamily), columnName))

		mutationBatch.statements << statement
	}

	void incrementCounterColumns(mutationBatch, columnFamily, rowKey, columnMap)
	{
		//mutationBatch.withRow(columnFamily, rowKey).incrementCounterColumns(columnMap)
		def valueKeyName = columnValueName(columnFamily)
		def rowKeyName = rowKeyName(columnFamily)
		def columnKeyName = columnKeyName(columnFamily)
		columnMap.each { columnName, value ->
			def statement = QueryBuilder.insertInto(tableName(columnFamily))
				.with(incr(valueKeyName, value))
				.where(eq(rowKeyName, rowKey))
				.and(QueryBuilder.in(columnKeyName, columnName))

			mutationBatch.statements << statement
		}
	}

	void deleteRow(mutationBatch, columnFamily, rowKey)
	{
		//mutationBatch.deleteRow([columnFamily], rowKey)
		def statement = QueryBuilder.delete()
			.from(tableName(columnFamily))
			.where(eq(rowKeyName(columnFamily), rowKey))

		mutationBatch.statements << statement
	}

	def execute(mutationBatch)
	{
		long t0 = System.currentTimeMillis()
		def batch = QueryBuilder.batch(*(mutationBatch.statements))
		def result = mutationBatch.session.execute(batch)
		logTime(t0, "execute")
		result
	}

	def getRow(rows, key)
	{
		//rows.getRow(key).columns
		rows[key].columns
	}

	// TODO - do we need this?
	//def getRowKey(row)
	//{
	//	row.key
	//}

	Iterable getColumns(row)
	{
		cqlRow.columns
	}

	def getColumn(cqlRow, name)
	{
		//row.getColumnByName(name)
		new CqlColumn(row: cqlRow.row, name: name, type: cqlRow.row.columnDefinitions.getType(name))
	}

	def getColumnByIndex(cqlRow, index)
	{
		//row.getColumnByIndex(index)
		new CqlColumn(row: cqlRow.row, name: getName(index), type: cqlRow.row.columnDefinitions.getType(index))
	}

	def name(column)
	{
		column.name
	}

	String stringValue(column)
	{
		column.stringValue
	}

	byte[] byteArrayValue(column)
	{
		column.byteArrayValue
	}

	def longValue(column)
	{
		column.longValue
	}

	UUID uuidValue(column)
	{
		column.UUIDValue
	}
/*
	private injectConsistencyLevel(query, ConsistencyLevel consistencyLevel)
	{
		if (consistencyLevel) {
			query.setConsistencyLevel(consistencyLevel)
		}
		return query
	}

	private injectConsistencyLevel(query, String consistencyLevel)
	{
		if (consistencyLevel) {
			def cl = ConsistencyLevel.valueOf(consistencyLevel)
			if (cl) {
				query.setConsistencyLevel(cl)
			}
			else {
				throw new IllegalArgumentException("'${consistencyLevel}' is not a valid ConsistencyLevel, must be one of [CL_ONE, CL_QUORUM, CL_ALL, CL_ANY, CL_EACH_QUORUM, CL_LOCAL_QUORUM, CL_TWO, CL_THREE]")
			}
		}
		return query
	}
*/
	static Boolean isUuidType(type) {
		type == "uuid"
	}

	static Boolean isTimeUuidType(type) {
		type == "timeuuid"
	}

	static Boolean isAnyUuidType(type) {
		["uuid","timeuuid"].contains(type)
	}

	private static String tableName(columnFamily) {
		"\"$columnFamily.name\""
	}

	private static String rowKeyName(columnFamily) {
		columnFamily.primaryKey[0].name
	}

	private static String rowKeyType(columnFamily) {
		columnFamily.primaryKey[0].type.name
	}

	private static rowKeyValue(columnFamily, row) {
		columnValue(row, rowKeyName(columnFamily), columnFamily.primaryKey[0].type)
	}

	private static String columnKeyName(columnFamily) {
		columnFamily.primaryKey[-1].name
	}

	private static String columnKeyType(columnFamily) {
		columnFamily.primaryKey[-1].type.name
	}

	private static String columnValueName(columnFamily) {
		"value" // TODO - get from metadata
	}

	private static rows(resultSet, columnFamily) {
		// TODO - Optimize DataMapping.makeResult() -- doing this because ORM passes in keys and the row list object
		def result = resultSet.all().collectEntries { row ->
			[rowKeyValue(columnFamily, row), new CqlRow(row: row, columnFamily: columnFamily)]
		}
		result
	}

	private static columnValue(row, name, type) {
		CqlColumn.columnValue(row, name, type)
	}

}
