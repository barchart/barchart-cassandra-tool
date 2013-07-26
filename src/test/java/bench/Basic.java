package bench;

import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Basic {

	private final Logger log = LoggerFactory.getLogger(getClass());

	public void main() throws Exception {

		final Config config = ConfigFactory.parseURL(new URL(
				"file:./src/test/resources/case-01/client.conf"));

		final Cluster cluster = Util.clusterFrom(config);

		final OperationResult<SchemaChangeResult> result = Util.keyspaceFrom(
				cluster, config);

		log.info("latency: {} ms", result.getLatency(TimeUnit.MILLISECONDS));

	}

	public static void main(final String[] args) throws Exception {

		new Basic().main();

	}

}
