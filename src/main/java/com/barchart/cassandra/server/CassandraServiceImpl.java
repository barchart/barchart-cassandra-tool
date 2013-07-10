package com.barchart.cassandra.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.barchart.cassandra.client.CassandraService;
import com.barchart.cassandra.shared.FieldVerifier;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * The server side implementation of the RPC service.
 */
@SuppressWarnings("serial")
public class CassandraServiceImpl extends RemoteServiceServlet implements
		CassandraService {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private AstyanaxContext<Keyspace> context = null;
	private Keyspace keyspace = null;

	public String greetServer(String input) throws IllegalArgumentException {

		// Verify that the input is valid.
		if (!FieldVerifier.isValidName(input)) {

			// If the input is not valid, throw an IllegalArgumentException back
			// to
			// the client.
			throw new IllegalArgumentException("Name must be a valid address");
		}

		String serverInfo = getServletContext().getServerInfo();
		String userAgent = getThreadLocalRequest().getHeader("User-Agent");

		// Escape data from the client to avoid cross-site script
		// vulnerabilities.
		input = escapeHtml(input);
		userAgent = escapeHtml(userAgent);

		try {
			context = new AstyanaxContext.Builder()
					.forCluster("ClusterName")
					.forKeyspace("KeyspaceName")
					.withAstyanaxConfiguration(
							new AstyanaxConfigurationImpl()
									.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
					.withConnectionPoolConfiguration(
							new ConnectionPoolConfigurationImpl(
									"MyConnectionPool").setPort(9160)
									.setMaxConnsPerHost(1)
									.setSeeds(input + ":9160"))
					.buildKeyspace(ThriftFamilyFactory.getInstance());

			context.start();
			keyspace = context.getClient();

			if (context != null)
				return "Successfully connected to " + input;

			return "Problems connecting to " + input;

		} catch (Exception e) {
			log.error( "Fatal error", e );
			return "Problems:\n" + e.getStackTrace();
		}
	}

	/**
	 * Escape an html string. Escaping data received from the client helps to
	 * prevent cross-site script vulnerabilities.
	 * 
	 * @param html
	 *            the html string to escape
	 * @return the escaped string
	 */
	private String escapeHtml(String html) {
		if (html == null) {
			return null;
		}
		return html.replaceAll("&", "&amp;").replaceAll("<", "&lt;")
				.replaceAll(">", "&gt;");
	}
}
