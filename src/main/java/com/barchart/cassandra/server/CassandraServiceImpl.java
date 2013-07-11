package com.barchart.cassandra.server;

import java.util.Calendar;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.barchart.cassandra.client.CassandraService;
import com.barchart.cassandra.shared.FieldVerifier;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * The server side implementation of the RPC service.
 */
@SuppressWarnings("serial")
public class CassandraServiceImpl extends RemoteServiceServlet implements
		CassandraService {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private static String KEYSPACE = "kerberos";
	private static String ACCOUNT_CREDENTIALS = "accountCredential";

	public String greetServer(String input) throws IllegalArgumentException {

		// Verify that the input is valid.
		if (!FieldVerifier.isValidName(input)) {

			// If the input is not valid, throw an IllegalArgumentException back
			// to the client.
			return "Name must be a valid address";
		}

		String userAgent = getThreadLocalRequest().getHeader("User-Agent");

		StringBuilder response = new StringBuilder();

		// Escape data from the client to avoid cross-site script
		// vulnerabilities.
		input = escapeHtml(input);
		userAgent = escapeHtml(userAgent);

		AstyanaxUtils.setProperty("seeds", input);

		if (AstyanaxUtils.getCluster() != null)
			try {

				response.append("Cluster properties are:\n"
						+ AstyanaxUtils.getCluster().describeClusterName()
						+ "\n"
						+ AstyanaxUtils.getCluster().describePartitioner()
						+ "\n" + AstyanaxUtils.getCluster().describeSnitch()
						+ "\n" + AstyanaxUtils.getCluster().getVersion());

				final Keyspace keyspace = AstyanaxUtils.getCluster()
						.getKeyspace(KEYSPACE);

				if (keyspace == null)
					response.append("\n\nNeed to create " + KEYSPACE);

				else
					response.append("\n\nKeyspace accountCredential found with properties\n"
							+ keyspace.describeSchemaVersions());

			} catch (ConnectionException e) {
				log.error("Error", e);
			}
		else
			response.append("Problems connecting to " + input);

		return response.toString();
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

	private ColumnFamily<String, String> CF_ACCOUNT_CREDENTIALS = new ColumnFamily<String, String>(
			ACCOUNT_CREDENTIALS, // Column Family Name
			StringSerializer.get(), // Key Serializer
			StringSerializer.get()); // Column Serializer

	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static Random rnd = new Random();

	static String randomString(int len) {
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++)
			sb.append(AB.charAt(rnd.nextInt(AB.length())));
		return sb.toString();
	}

	@Override
	public String batchInsertUsers(Integer number, Integer batchNum)
			throws IllegalArgumentException {

		StringBuilder response = new StringBuilder();
		Keyspace keyspace = null;

		try {
			keyspace = AstyanaxUtils.getCluster().getKeyspace(KEYSPACE);

		} catch (ConnectionException e) {
			log.error("Error", e);
		}

		if (keyspace == null)
			response.append("\n\nNeed to create " + KEYSPACE);

		else {
			long begin = Calendar.getInstance().getTimeInMillis();

			for ( int i = 0; i < number; i += batchNum ) {
				MutationBatch m = keyspace.prepareMutationBatch();

				for ( int j = 0; j < batchNum; j++ ) {
					final String id = randomString(32);
					m.withRow(CF_ACCOUNT_CREDENTIALS, randomString(32) )
							.putColumn("id", id, null)
							.putColumn("uri", "http://secure.barchart.com/" + id, null);
				}

				// TBD - Latency etc
				try {
					OperationResult<Void> result = m.execute();

				} catch (ConnectionException e) {
				}
			}

			long end = Calendar.getInstance().getTimeInMillis();
			response.append( "\nTotal time was " + ( end - begin ) / 1000 + " sec" );
		}

		try {
			OperationResult<CqlResult<String, String>> result
				= keyspace.prepareQuery( CF_ACCOUNT_CREDENTIALS )
					.withCql("SELECT count(*) FROM \"" + ACCOUNT_CREDENTIALS + "\";" )
					.execute();

			response.append( "\n" + CF_ACCOUNT_CREDENTIALS + " now holds " + result.getResult().getNumber() + " entries" );

		} catch (ConnectionException e) {
			log.error("Error", e);
			response.append( "\n" + e );
		}
		
		return response.toString();
	}
}
