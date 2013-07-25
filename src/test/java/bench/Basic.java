package bench;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Basic {

	private final Logger log = LoggerFactory.getLogger(getClass());

	public void main() throws Exception {

		final Config config = ConfigFactory.parseURL(new URL(
				"file:./src/test/resources/case-01/client.conf"));

		final Cluster cluster = Util.clusterFrom(config);

		final Map<String, String> options = new HashMap<String, String>();
		options.put("eqx", "2");
		options.put("us-east-1", "2");
		options.put("us-west-1", "2");

		final KeyspaceDefinition keyspace = cluster.makeKeyspaceDefinition();
		keyspace.setName("andrei_001");
		keyspace.setStrategyClass("org.apache.cassandra.locator.NetworkTopologyStrategy");
		keyspace.setStrategyOptions(options);

		final OperationResult<SchemaChangeResult> result = cluster
				.addKeyspace(keyspace);

		log.info("latency: {} ms", result.getLatency(TimeUnit.MILLISECONDS));

	}

	public static void main(final String[] args) throws Exception {

		new Basic().main();

	}

}
