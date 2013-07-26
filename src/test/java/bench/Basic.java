package bench;

import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.MutationBatch;
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

		final OperationResult<SchemaChangeResult> keyspaceResult = Util
				.keyspaceFrom(cluster, config);

		log.info("keyspace: {} ms",
				keyspaceResult.getLatency(TimeUnit.MILLISECONDS));

		final OperationResult<SchemaChangeResult> tableResult = Util.tableFrom(
				cluster, config);

		log.info("table: {} ms", tableResult.getLatency(TimeUnit.MILLISECONDS));

		long batchSize = 0;
		long batchTime = 0;

		for (int batch = 0; batch < 100; batch++) {

			final MutationBatch mutate = Util.mutationFrom(cluster, config);
			batchSize += mutate.getRowCount();
			log.info("batchSize: {}", batchSize);

			final OperationResult<Void> mutateResult = mutate.execute();
			batchTime += mutateResult.getLatency(TimeUnit.MILLISECONDS);
			log.info("batchTime: {}", batchTime);

		}

		log.info("mutate: {} ms", batchTime / batchSize);

	}

	public static void main(final String[] args) throws Exception {

		new Basic().main();

	}

}
