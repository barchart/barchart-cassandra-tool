package com.barchart.cassandra.server;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class ClusterLoader {

	protected final Logger log = LoggerFactory.getLogger(getClass());
	private static AstyanaxContext<Cluster> clusterContext = null;
	private static Cluster cluster = null;

	public ClusterLoader() {
	}

	public Cluster newCluster(String clusterName, String seedHosts,
			String connectionPoolName, int maxConns, int maxConnsPerHost,
			int maxTimeoutCount, int connectTimeout, int socketTimeout) {

		log.info("******  Create Cluster [" + clusterName + "]  ******");
		log.info("connectionPoolName :" + connectionPoolName);
		log.info("seedHosts          :" + seedHosts);
		log.info("maxConns           :" + maxConns);
		log.info("maxConnsPerHost    :" + maxConnsPerHost);
		log.info("connectTimeout     :" + connectTimeout);
		log.info("socketTimeout      :" + socketTimeout);
		log.info("maxTimeoutCount    :" + maxTimeoutCount);

		// Init builder - We connect to one node we connect to them all anyways
		if ( clusterContext == null ) {
			ConnectionPoolConfigurationImpl connectionPoolConfiguration =
					new ConnectionPoolConfigurationImpl( connectionPoolName )
						.setSeeds(seedHosts)
						.setMaxConns(maxConns)
						.setMaxConnsPerHost(maxConnsPerHost)
						.setConnectTimeout(connectTimeout)
						.setSocketTimeout(socketTimeout)
						.setMaxTimeoutCount(maxTimeoutCount)
				
						// MJS: Added those to solidify the connection as I get a timeout quite often
						.setLatencyAwareUpdateInterval(10000)  // Will resort hosts per token partition every 10 seconds
				        .setLatencyAwareResetInterval(10000) // Will clear the latency every 10 seconds. In practice I set this to 0 which is the default. It's better to be 0.
				        .setLatencyAwareBadnessThreshold(2) // Will sort hosts if a host is more than 100% slower than the best and always assign connections to the fastest host, otherwise will use round robin
				        .setLatencyAwareWindowSize(100); // Uses last 100 latency samples. These samples are in a FIFO q and will just cycle themselves.

			Builder builder = new AstyanaxContext.Builder()
						.forCluster(clusterName)
						.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
				        	.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)							// https://github.com/Netflix/astyanax/issues/127
				        	.setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)						// https://github.com/Netflix/astyanax/issues/127
				        	.setCqlVersion("3.0.0")
							.setTargetCassandraVersion("1.2"))
						.withConnectionPoolConfiguration(connectionPoolConfiguration)
						.withConnectionPoolMonitor(new CountingConnectionPoolMonitor());

			// get cluster
			clusterContext = builder.buildCluster(ThriftFamilyFactory.getInstance());

			clusterContext.start();
		}

		return clusterContext.getClient();
	}

	public synchronized Cluster getCluster(final Map<String, String> map) {

		if (cluster != null)
			return cluster;

		try {
			String clusterName = map.get("cluster");
			String seedHosts = map.get("seeds");
			String connectionPoolName = map.get("connection.pool.name");
			int maxConns = Integer.parseInt(map.get("max.conns"));
			int maxConnsPerHost = Integer.parseInt( map.get("max.conns.per.host") );
			int maxTimeoutCount = Integer
					.parseInt(map.get("max.timeout.count"));
			int connectTimeout = Integer.parseInt(map.get("connect.timeout"));
			int socketTimeout = Integer.parseInt(map.get("socket.timeout"));

			cluster = newCluster(clusterName, seedHosts, connectionPoolName,
					maxConns, maxConnsPerHost, maxTimeoutCount, connectTimeout,
					socketTimeout);

		} catch (Exception e) {
			log.error("%%%% Error Loading 'cluster.properties' %%%%", e);
			return null;
		}

		return cluster;
	}

	public static synchronized void endCluster() {

		if ( clusterContext != null ) {
			clusterContext.shutdown();
			clusterContext = null;
		}
	}
}