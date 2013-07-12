package com.barchart.cassandra.server;

import java.util.Calendar;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.barchart.cassandra.client.CassandraService;
import com.barchart.cassandra.shared.FieldVerifier;
import com.google.common.collect.ImmutableMap;
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

	// MJS: All column names are lowercase as this is how they are created and SQL3 is case sensitive
	private static String ACCOUNT_BILLING = "accountbilling";
	private static String ACCOUNT_CREDENTIALS = "accountcredential";

	private ColumnFamily<String, String> CF_ACCOUNT_CREDENTIALS = new ColumnFamily<String, String>(
			ACCOUNT_CREDENTIALS,		// Column Family Name
			StringSerializer.get(),		// Key Serializer
			StringSerializer.get());	// Column Serializer

	private static String ACCOUNT_INFORMATION = "accountinformation";
	private static String ACCOUNT_URI_SEARCH = "accounturisearch";
	private static String ACCOUNT_URI_TO_ID = "accounturitoid";

	public String connect(String input) throws IllegalArgumentException {

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

				response.append("SUCCESS\nCluster properties are:\n"
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
				response.append( "ERROR\n" + e );
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
			long latencies = 0;
			long begin = Calendar.getInstance().getTimeInMillis();

			for ( int i = 0; i < number; i += batchNum ) {
				MutationBatch m = keyspace.prepareMutationBatch();

				for ( int j = 0; j < batchNum; j++ ) {
					final String id = randomString(32);
					m.withRow(CF_ACCOUNT_CREDENTIALS, randomString(32) )
							.putColumn("id", id, null)
							.putColumn("uri", "http://secure.barchart.com/" + id, null);
				}

				try {
					OperationResult<Void> result = m.execute();
					latencies += result.getLatency();

				} catch (ConnectionException e) {
				}
			}

			long end = Calendar.getInstance().getTimeInMillis();
			response.append( "\nTotal time was " + ( end - begin ) / 1000 + " sec\nAverage latency of a batch was " + latencies / ( number / batchNum ) + " ms" );
		}

		try {
			OperationResult<CqlResult<String, String>> result
				= keyspace.prepareQuery( CF_ACCOUNT_CREDENTIALS )
					.withCql("SELECT count(*) FROM " + ACCOUNT_CREDENTIALS + " LIMIT 10000000;" )
					.execute();

			// MJS: Pretty is ain't
			response.append( "\n\n" + CF_ACCOUNT_CREDENTIALS + " now holds " + result.getResult().getRows().getRowByIndex(0).getColumns().getColumnByName("count").getLongValue() + " entries" );

		} catch (ConnectionException e) {
			log.error("Error", e);
			response.append( "\n" + e );
		}
		
		return response.toString();
	}

	@Override
	public String createSchema() {

		StringBuilder response = new StringBuilder();

		AstyanaxUtils.dropColumnFamily( KEYSPACE, ACCOUNT_BILLING );
		AstyanaxUtils.dropColumnFamily( KEYSPACE, ACCOUNT_CREDENTIALS );
		AstyanaxUtils.dropColumnFamily( KEYSPACE, ACCOUNT_INFORMATION );
		AstyanaxUtils.dropColumnFamily( KEYSPACE, ACCOUNT_URI_SEARCH );
		AstyanaxUtils.dropColumnFamily( KEYSPACE, ACCOUNT_URI_TO_ID );
		AstyanaxUtils.dropKeyspace( KEYSPACE );

		response.append( "Dropped all the column families and keyspace\n" );

		AstyanaxUtils.createKeyspace( KEYSPACE, AstyanaxUtils.SimpleStrategy, 2 );

		try {
			Keyspace keyspace = AstyanaxUtils.getCluster().getKeyspace(KEYSPACE);

			keyspace.createColumnFamily(CF_ACCOUNT_CREDENTIALS, ImmutableMap.<String, Object>builder()

					// MJS: Overriding types to UTF-8
					.put("default_validation_class", "UTF8Type")
			        .put("key_validation_class",     "UTF8Type")
			        .put("comparator_type",          "UTF8Type")

			        // MJS: Indexes
			        .put("column_metadata", ImmutableMap.<String, Object>builder()
				            .put("key", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .put("index_name",       "key")
				                .put("index_type",       "KEYS")
				                .build())
				            .put("id", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .put("index_name",       "id")
				                .put("index_type",       "KEYS")
				                .build())
				            .put("uri", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .put("index_name",       "uri")
				                .put("index_type",       "KEYS")
				                .build())
				            .build())
				        .build());

			response.append( "Regenerated all the column families and keyspace\n" );

		} catch (ConnectionException e) {
			log.error("Error", e);
			response.append( "\n" + e );
		}
	    
		return response.toString();
	}
}
