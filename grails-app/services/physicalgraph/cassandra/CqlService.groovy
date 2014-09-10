package physicalgraph.cassandra

import com.datastax.driver.core.Cluster
import org.springframework.beans.factory.InitializingBean

class CqlService implements InitializingBean
{
	boolean transactional = false

	def grailsApplication

	def clusters
	String defaultCluster
	String defaultKeyspace

	private clusterMap = [:]

	/**
	 * Provides persistence methods for cassandra-orm plugin
	 */
	def orm

	void afterPropertiesSet()
	{
		def config = grailsApplication.config.cassandra.cql
		orm = new CqlPersistenceMethods(log: log)
		clusters = config.clusters
		defaultCluster = config.defaultCluster
		defaultKeyspace = clusters[defaultCluster].defaultKeyspace ?: config.defaultKeyspace

		clusters.each { key, props ->
			clusterMap[key] = props +
				[
					defaultKeyspace: props.defaultKeyspace ?: config.defaultKeyspace
				]
		}


	}

	def defaultKeyspaceName(cluster)
	{
		clusterMap[cluster].defaultKeyspace
	}

	def metadata(String cluster = defaultCluster)
	{
		def clusterHandle = clusterMap[cluster].handle
		if (!clusterHandle) {
			clusterHandle = newClusterHandle(cluster)
		}
		return clusterHandle.metadata
	}

	def keyspaceMetadata(String keyspace = null, String cluster = defaultCluster)
	{
		def ks = keyspace ?: clusterMap[cluster].defaultKeyspace
		metadata(cluster).getKeyspace(ks)
	}

	/**
	 * Execute a CQL command against the default cluster and keyspace
	 */
	def execute(String cluster=defaultCluster, cmd) {
		def clusterHandle = clusterMap[cluster].handle
		if (!clusterHandle) {
			clusterHandle = newClusterHandle(cluster)
		}
		return clusterHandle.connect().execute(cmd)
	}

	/**
	 * Returns a CQL session
	 *
	 * @param name Optional, ame of the keyspace, defaults to configured defaultKeyspace
	 * @param cluster Optional, name of the Cassandra cluster, defaults to configured defaultCluster
	 *
	 */
	def keyspace(String name=null, String cluster=defaultCluster)
	{
		context(name, cluster)
	}

	/**
	 * Constructs a CQL session and passes execution to a closure
	 *
	 * @param name Optional, ame of the keyspace, defaults to configured defaultKeyspace
	 * @param cluster Optional, name of the Cassandra cluster, defaults to configured defaultCluster
	 *
	 */
	def withKeyspace(String keyspace=null, String cluster=defaultCluster, Closure block) throws Exception
	{
		block(context(keyspace, cluster))
	}

	/**
	 * Creates a CQL session for the specified cluster and keyspace
	 *
	 * @param keyspace name of the keyspace
	 * @param cluster name of the cluster
	 *
	 */
	def context(keyspace, cluster)
	{
		def ks = keyspace ?: clusterMap[cluster].defaultKeyspace
		def clusterHandle = clusterMap[cluster].handle
		if (!clusterHandle) {
			clusterHandle = newClusterHandle(cluster)
		}
		return clusterHandle.connect(ks)
	}

	/**
	 * Constructs new context and stores it in the map
	 */
	private synchronized newClusterHandle(cluster)
	{
		def entry = clusterMap[cluster]
		def handle = entry.handle
		if (!handle) {

			def props = clusters[cluster]

			Cluster.Builder clusterBuilder = Cluster.builder()

			clusterBuilder.addContactPoints(props.seeds)

			if (props.username && props.password) {
				clusterBuilder.withCredentials(props.username, props.password)
			}

			if (props.poolingOptions) {
				clusterBuilder.withPoolingOptions(props.poolingOptions)
			}

			if (props.socketOptions) {
				clusterBuilder.withPoolingOptions(props.socketOptions)
			}

			if (props.protocolOptions) {
				clusterBuilder.withPoolingOptions(props.protocolOptions)
			}

			if (props.reconnectionPolicy) {
				clusterBuilder.withReconnectionPolicy(props.reconnectionPolicy)
			}

			if (props.retryPolicy) {
				clusterBuilder.withReconnectionPolicy(props.retryPolicy)
			}

			if (props.sslOptions) {
				clusterBuilder.withSSL(props.sslOptions)
			}
			else if (props.withSSL) {
				clusterBuilder.withSSL()
			}

			// TODO -- maybe use withDeferredInitialization and not have to do this?
			if (props.withDeferredInitialization) {
				clusterBuilder.withDeferredInitialization()
			}

			if (props.compression) {
				clusterBuilder.withCompression(props.compression)
			}

			entry.handle = clusterBuilder.build()
		}
		return entry.handle
	}

}
