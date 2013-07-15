package com.barchart.cassandra.server;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
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

		// init builder
		ConnectionPoolConfigurationImpl connectionPoolConfiguration = new ConnectionPoolConfigurationImpl(
				connectionPoolName);
		log.info("connectionPoolName :" + connectionPoolName);

		connectionPoolConfiguration.setSeeds(seedHosts);
		log.info("seedHosts          :" + seedHosts);

		connectionPoolConfiguration.setMaxConns(maxConns);
		log.info("maxConns           :" + maxConns);

		connectionPoolConfiguration.setMaxConnsPerHost(maxConnsPerHost);
		log.info("maxConnsPerHost    :" + maxConnsPerHost);

		connectionPoolConfiguration.setConnectTimeout(connectTimeout);
		log.info("connectTimeout     :" + connectTimeout);

		connectionPoolConfiguration.setSocketTimeout(socketTimeout);
		log.info("socketTimeout      :" + socketTimeout);

		connectionPoolConfiguration.setMaxTimeoutCount(maxTimeoutCount);
		log.info("maxTimeoutCount    :" + maxTimeoutCount);

		Builder builder = new AstyanaxContext.Builder();
		builder.forCluster(clusterName).withAstyanaxConfiguration(
				new AstyanaxConfigurationImpl()
						.setDiscoveryType(NodeDiscoveryType.NONE));
		builder.withConnectionPoolConfiguration(connectionPoolConfiguration);
		builder.withConnectionPoolMonitor(new CountingConnectionPoolMonitor());

		// get cluster
		clusterContext = builder
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()      
							.setCqlVersion("3.0.0")
							.setTargetCassandraVersion("1.2"))
				.buildCluster(ThriftFamilyFactory.getInstance());

		clusterContext.start();
		Cluster cluster = clusterContext.getClient();

		return cluster;
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