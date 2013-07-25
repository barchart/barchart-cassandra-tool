package bench;

import java.util.HashMap;
import java.util.Map;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.typesafe.config.Config;

class Util {

	public static ConnectionPoolConfiguration connectFrom(final Config root) {

		final Config config = root.getConfig("ConnectionPoolConfiguration");

		final ConnectionPoolConfigurationImpl entry = new ConnectionPoolConfigurationImpl(
				config.getString("PoolName"));

		entry.setMaxConnsPerHost(config.getInt("MaxConnsPerHost"));

		entry.setSeeds(config.getString("Seeds"));

		return entry;
	}

	public static AstyanaxConfiguration configFrom(final Config root) {

		final Config config = root.getConfig("AstyanaxConfiguration");

		final AstyanaxConfigurationImpl entry = new AstyanaxConfigurationImpl();

		entry.setDiscoveryType(NodeDiscoveryType.valueOf(config
				.getString("DiscoveryType")));

		return entry;
	}

	public static Cluster clusterFrom(final Config root) {

		final Config config = root.getConfig("AstyanaxContext");

		final AstyanaxContext<Cluster> context = new AstyanaxContext.Builder()
				.forCluster(config.getString("Cluster"))
				.withAstyanaxConfiguration(configFrom(config))
				.withConnectionPoolConfiguration(connectFrom(config))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildCluster(ThriftFamilyFactory.getInstance());

		context.start();

		return context.getClient();
	}

	public static OperationResult<SchemaChangeResult> keyspaceFrom(
			final Cluster cluster, final Config root) throws Exception {

		final Config config = root.getConfig("KeyspaceDefinition");

		final Map<String, String> options = new HashMap<String, String>();
		options.put("eqx", "2");
		options.put("us-east-1", "2");
		options.put("us-west-1", "2");

		final KeyspaceDefinition keyspace = cluster.makeKeyspaceDefinition();

		keyspace.setName(config.getString("Name"));

		keyspace.setStrategyClass("org.apache.cassandra.locator.NetworkTopologyStrategy");

		keyspace.setStrategyOptions(options);

		final OperationResult<SchemaChangeResult> result = cluster
				.addKeyspace(keyspace);

		return result;

	}

}
