package physicalgraph.cassandra


class CqlMutationBatch
{
	def session
	def statements = []
	def consistencyLevel
}
