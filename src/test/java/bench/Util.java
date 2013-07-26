package bench;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.thrift.ddl.ThriftColumnDefinitionImpl;
import com.typesafe.config.Config;

class Util {

	public static ConnectionPoolConfiguration connectFrom(final Config root) {

		final Config config = root.getConfig("connection_pool_configuration");

		final ConnectionPoolConfigurationImpl entry = new ConnectionPoolConfigurationImpl(
				config.getString("pool_name"));

		entry.setMaxConnsPerHost(config.getInt("max_conns_per_host"));

		entry.setSeeds(config.getString("seeds"));

		return entry;
	}

	public static AstyanaxConfiguration configFrom(final Config root) {

		final Config config = root.getConfig("astyanax_configuration");

		final AstyanaxConfigurationImpl entry = new AstyanaxConfigurationImpl();

		entry.setDiscoveryType(NodeDiscoveryType.valueOf(config
				.getString("discovery_type")));

		return entry;
	}

	public static Cluster clusterFrom(final Config root) {

		final Config config = root.getConfig("astyanax_context");

		final AstyanaxContext<Cluster> context = new AstyanaxContext.Builder()
				.forCluster(config.getString("cluster"))
				.withAstyanaxConfiguration(configFrom(config))
				.withConnectionPoolConfiguration(connectFrom(config))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildCluster(ThriftFamilyFactory.getInstance());

		context.start();

		return context.getClient();
	}

	public static Map<String, String> mapFrom(final Config config) {
		final Map<String, Object> source = config.root().unwrapped();
		final Map<String, String> target = new HashMap<String, String>();
		for (final Map.Entry<String, Object> entry : source.entrySet()) {
			target.put(entry.getKey(), entry.getValue().toString());
		}
		return target;
	}

	public static ColumnDefinition columnFrom(final Config config) {

		final ColumnDefinition column = new ThriftColumnDefinitionImpl();

		column.setName(config.getString("name"));

		column.setValidationClass(config.getString("validation_class"));

		return column;

	}

	public static OperationResult<SchemaChangeResult> tableFrom(
			final Cluster cluster, final Config root) throws Exception {

		final Config config = root.getConfig("column_family_definition");

		final ColumnFamilyDefinition table = cluster
				.makeColumnFamilyDefinition();

		table.setKeyspace(config.getString("keyspace"));

		table.setName(config.getString("name"));
		table.setComment(config.getString("comment"));

		table.setComparatorType(config.getString("comparator_type"));

		table.setKeyValidationClass(config.getString("key_validation_class"));
		table.setDefaultValidationClass(config
				.getString("default_validation_class"));

		table.setCompactionStrategy(config.getString("compaction_strategy"));

		final List<? extends Config> columnList = config
				.getConfigList("column_definition_list");

		for (final Config column : columnList) {
			table.addColumnDefinition(columnFrom(column));
		}

		final OperationResult<SchemaChangeResult> result = cluster
				.addColumnFamily(table);

		return result;

	}

	public static OperationResult<SchemaChangeResult> keyspaceFrom(
			final Cluster cluster, final Config root) throws Exception {

		final KeyspaceDefinition keyspace = cluster.makeKeyspaceDefinition();

		final Config config = root.getConfig("keyspace_definition");

		keyspace.setName(config.getString("name"));

		final Config strategy = config.getConfig("strategy");

		keyspace.setStrategyClass(strategy.getString("class"));

		keyspace.setStrategyOptions(mapFrom(strategy.getConfig("options")));

		final OperationResult<SchemaChangeResult> result = cluster
				.addKeyspace(keyspace);

		return result;

	}

	public static MutationBatch mutationFrom(final Cluster cluster,
			final Config root) throws Exception {

		final Config config = root.getConfig("mutation_batch");

		final Keyspace keyspace = cluster.getKeyspace(config
				.getString("keyspace"));

		final MutationBatch batch = keyspace.prepareMutationBatch();

		batch.withConsistencyLevel(ConsistencyLevel.valueOf(config
				.getString("consitency_level")));

		final ColumnFamily<String, String> table = new ColumnFamily<String, String>(
				config.getString("table_name"), StringSerializer.get(),
				StringSerializer.get());

		final List<? extends Config> columnList = config
				.getConfigList("column_list");

		final int rowCount = config.getInt("row_count");

		final String batchKey = new DateTime().toString();

		for (int row = 0; row < rowCount; row++) {

			final String rowKey = batchKey + "/" + row;

			final ColumnListMutation<String> change = batch.withRow(table,
					rowKey);

			for (final Config column : columnList) {
				change.putColumn(column.getString("name"),
						column.getString("value"), null);
			}

		}

		batch.withTimestamp(System.currentTimeMillis());

		return batch;

	}

}
