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
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;

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

	private ColumnFamily<String, String> CF_ACCOUNT_BILLING = new ColumnFamily<String, String>(
			ACCOUNT_BILLING,			// Column Family Name
			StringSerializer.get(),		// Key Serializer
			StringSerializer.get());	// Column Serializer

	private static String ACCOUNT_CREDENTIALS = "accountcredential";

	private ColumnFamily<String, String> CF_ACCOUNT_CREDENTIALS = new ColumnFamily<String, String>(
			ACCOUNT_CREDENTIALS,		// Column Family Name
			StringSerializer.get(),		// Key Serializer
			StringSerializer.get());	// Column Serializer

	private static String ACCOUNT_INFORMATION = "accountinformation";

	private ColumnFamily<String, String> CF_ACCOUNT_INFORMATION = new ColumnFamily<String, String>(
			ACCOUNT_INFORMATION,		// Column Family Name
			StringSerializer.get(),		// Key Serializer
			StringSerializer.get());	// Column Serializer

	private static String ACCOUNT_URI_SEARCH = "accounturisearch";

	private ColumnFamily<String, String> CF_ACCOUNT_URI_SEARCH = new ColumnFamily<String, String>(
			ACCOUNT_URI_SEARCH,		// Column Family Name
			StringSerializer.get(),		// Key Serializer
			StringSerializer.get());	// Column Serializer

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

			} catch ( Exception e ) {
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
					final String firstName = randomString(32);
					final String lastName = randomString(32);
					final String addressCity = randomString(32);
					final String addressCompany = randomString(32);
					final String addressCountry = randomString(32);
					final String addressState = randomString(32);
					final String addressStreet = randomString(32);
					final String addressZip = randomString(32);

					m.withRow(CF_ACCOUNT_CREDENTIALS, randomString(32))
							.putColumn("id", id, null)
							.putColumn("uri",
									"http://secure.barchart.com/" + lastName + "." + firstName, null);

					m.withRow(CF_ACCOUNT_INFORMATION, randomString(32))
							.putColumn("id_info", id, null)
							.putColumn("address_city", addressCity, null)
							.putColumn("address_company", addressCompany,
									null)
							.putColumn("address_country", addressCountry,
									null)
							.putColumn("address_state", addressState, null)
							.putColumn("address_street", addressStreet, null)
							.putColumn("address_zip", addressZip, null)
							.putColumn("contact_chat_gtalk", randomString(32),
									null)
							.putColumn("contact_chat_icq", randomString(32),
									null)
							.putColumn("contact_chat_jabber", randomString(32),
									null)
							.putColumn("contact_email_main", randomString(32),
									null)
							.putColumn("contact_email_next", randomString(32),
									null)
							.putColumn("contact_phone_business",
									randomString(32), null)
							.putColumn("contact_phone_fax", randomString(32),
									null)
							.putColumn("contact_phone_home", randomString(32),
									null)
							.putColumn("contact_phone_mobile",
									randomString(32), null)
							.putColumn("name_first", firstName, null)
							.putColumn("name_last", lastName, null);

					m.withRow(CF_ACCOUNT_BILLING, randomString(32))
					.putColumn("id_bill", id, null)
					.putColumn("address_city", addressCity, null)
					.putColumn("address_company", addressCompany,
							null)
					.putColumn("address_country", addressCountry,
							null)
					.putColumn("address_state", addressState, null)
					.putColumn("address_street", addressStreet, null)
					.putColumn("address_zip", addressZip, null)
					.putColumn("card_code", randomString(32),
							null)
					.putColumn("card_expire", randomString(32),
							null)
					.putColumn("card_number", randomString(32),
							null)
					.putColumn("card_type", randomString(32),
							null)
					.putColumn("name_full", firstName + " " + lastName,
							null);

					m.withRow( CF_ACCOUNT_URI_SEARCH, randomString(32) )
					.putColumn( "user", lastName + "." + firstName, null )
					.putColumn( "uri_search", "http://secure.barchart.com/" + lastName + "." + firstName, null )
					.putColumn( "id_search", id, null );
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
			final OperationResult<CqlResult<String, String>> result1
				= keyspace.prepareQuery( CF_ACCOUNT_CREDENTIALS )
					.withCql("SELECT count(*) FROM " + ACCOUNT_CREDENTIALS + " LIMIT 10000000;" )
					.execute();

			// MJS: Pretty is ain't
			response.append( "\n\n" + ACCOUNT_CREDENTIALS + " now holds " + result1.getResult().getRows().getRowByIndex(0).getColumns().getColumnByName("count").getLongValue() + " entries" );

			final OperationResult<CqlResult<String, String>> result2
			= keyspace.prepareQuery( CF_ACCOUNT_INFORMATION )
				.withCql("SELECT count(*) FROM " + ACCOUNT_INFORMATION + " LIMIT 10000000;" )
				.execute();

			// MJS: Pretty is ain't
			response.append( "\n\n" + ACCOUNT_INFORMATION + " now holds " + result2.getResult().getRows().getRowByIndex(0).getColumns().getColumnByName("count").getLongValue() + " entries" );

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
		AstyanaxUtils.dropKeyspace( KEYSPACE );

		response.append( "Dropped all the column families and keyspace\n" );

		AstyanaxUtils.createKeyspace( KEYSPACE, AstyanaxUtils.SimpleStrategy, 2 );

		try {
			final Keyspace keyspace = AstyanaxUtils.getCluster().getKeyspace(KEYSPACE);

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

			keyspace.createColumnFamily(CF_ACCOUNT_INFORMATION, ImmutableMap.<String, Object>builder()

					// MJS: Overriding types to UTF-8
					.put("default_validation_class", "UTF8Type")
			        .put("key_validation_class",     "UTF8Type")
			        .put("comparator_type",          "UTF8Type")

			        // MJS: Indexes
			        .put("column_metadata", ImmutableMap.<String, Object>builder()
				            .put("id_info", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .put("index_name",       "id_info")
				                .put("index_type",       "KEYS")
				                .build())
				            .put("address_city", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("address_company", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("address_country", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("address_state", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("address_street", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("address_zip", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("contact_chat_gtalk", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("contact_chat_icq", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("contact_chat_jabber", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("contact_email_main", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("contact_email_next", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("contact_phone_business", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("contact_phone_fax", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("contact_phone_home", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("contact_phone_mobile", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("name_first", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("name_last", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .build())
				        .build());

			keyspace.createColumnFamily(CF_ACCOUNT_BILLING, ImmutableMap.<String, Object>builder()

					// MJS: Overriding types to UTF-8
					.put("default_validation_class", "UTF8Type")
			        .put("key_validation_class",     "UTF8Type")
			        .put("comparator_type",          "UTF8Type")

			        // MJS: Indexes
			        .put("column_metadata", ImmutableMap.<String, Object>builder()
				            .put("id_bill", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .put("index_name",       "id_bill")
				                .put("index_type",       "KEYS")
				                .build())
				            .put("address_city", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("address_company", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("address_country", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("address_state", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("address_street", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("address_zip", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("card_code", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("card_expire", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("card_number", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("card_type", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .put("full_name", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .build())
				            .build())
				        .build());

			keyspace.createColumnFamily(CF_ACCOUNT_URI_SEARCH, ImmutableMap.<String, Object>builder()

					// MJS: Overriding types to UTF-8
					.put("default_validation_class", "UTF8Type")
			        .put("key_validation_class",     "UTF8Type")
			        .put("comparator_type",          "UTF8Type")

			        // MJS: Indexes
			        .put("column_metadata", ImmutableMap.<String, Object>builder()
				            .put("user", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .put("index_name",       "user")
				                .put("index_type",       "KEYS")
				                .build())
				            .put("id_search", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .put("index_name",       "id_search")
				                .put("index_type",       "KEYS")
				                .build())
				            .put("uri_search", ImmutableMap.<String, Object>builder()
				                .put("validation_class", "UTF8Type")
				                .put("index_name",       "uri_search")
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

	@Override
	public String batchModifyUsers(Integer number, Integer batchNum) {
		StringBuilder response = new StringBuilder();
		Keyspace keyspace = null;

		try {
			keyspace = AstyanaxUtils.getCluster().getKeyspace(KEYSPACE);

		} catch (ConnectionException e) {
			log.error("Error", e);
		}

		if ( keyspace == null )
			response.append("\n\nNeed to create " + KEYSPACE);

		else {
			long latencies = 0;
			long begin = Calendar.getInstance().getTimeInMillis();

			// MJS: Get the keys for all the users
			Rows<String, String> rows = null;

			try {
				rows = keyspace.prepareQuery(CF_ACCOUNT_INFORMATION)
						  .getAllRows()
						  .withColumnRange(new RangeBuilder().setLimit(0).build())
						  .execute().getResult();

			} catch (ConnectionException e1) {
				response.append( "\n\nCouldn't get user keys" );
				return response.toString();
			}

			String[] ids = null;
			ids = rows.getKeys().toArray( ids );

			// MJS: Here we redefine key parameters and affect to the max the given users
			for ( int i = 0; i < number; i += batchNum ) {
				MutationBatch m = keyspace.prepareMutationBatch();

				for ( int j = 0; j < batchNum; j++ ) {

					final String id = ids[new Random().nextInt( ids.length )];
					final String firstName = randomString(32);
					final String lastName = randomString(32);
					final String addressCity = randomString(32);
					final String addressCompany = randomString(32);
					final String addressCountry = randomString(32);
					final String addressState = randomString(32);
					final String addressStreet = randomString(32);
					final String addressZip = randomString(32);

					m.withRow(CF_ACCOUNT_CREDENTIALS, randomString(32))
							.putColumn("id", id, null)
							.putColumn("uri",
									"http://secure.barchart.com/" + lastName + "." + firstName, null);

					m.withRow(CF_ACCOUNT_INFORMATION, randomString(32))
							.putColumn("id_info", id, null)
							.putColumn("address_city", addressCity, null)
							.putColumn("address_company", addressCompany,
									null)
							.putColumn("address_country", addressCountry,
									null)
							.putColumn("address_state", addressState, null)
							.putColumn("address_street", addressStreet, null)
							.putColumn("address_zip", addressZip, null)
							.putColumn("contact_chat_gtalk", randomString(32),
									null)
							.putColumn("contact_chat_icq", randomString(32),
									null)
							.putColumn("contact_chat_jabber", randomString(32),
									null)
							.putColumn("contact_email_main", randomString(32),
									null)
							.putColumn("contact_email_next", randomString(32),
									null)
							.putColumn("contact_phone_business",
									randomString(32), null)
							.putColumn("contact_phone_fax", randomString(32),
									null)
							.putColumn("contact_phone_home", randomString(32),
									null)
							.putColumn("contact_phone_mobile",
									randomString(32), null)
							.putColumn("name_first", firstName, null)
							.putColumn("name_last", lastName, null);

					m.withRow(CF_ACCOUNT_BILLING, randomString(32))
					.putColumn("id_bill", id, null)
					.putColumn("address_city", addressCity, null)
					.putColumn("address_company", addressCompany,
							null)
					.putColumn("address_country", addressCountry,
							null)
					.putColumn("address_state", addressState, null)
					.putColumn("address_street", addressStreet, null)
					.putColumn("address_zip", addressZip, null)
					.putColumn("card_code", randomString(32),
							null)
					.putColumn("card_expire", randomString(32),
							null)
					.putColumn("card_number", randomString(32),
							null)
					.putColumn("card_type", randomString(32),
							null)
					.putColumn("name_full", firstName + " " + lastName,
							null);

					m.withRow( CF_ACCOUNT_URI_SEARCH, randomString(32) )
					.putColumn( "user", lastName + "." + firstName, null )
					.putColumn( "uri_search", "http://secure.barchart.com/" + lastName + "." + firstName, null )
					.putColumn( "id_search", id, null );
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
		
		return response.toString();
	}

	@Override
	public String disconnect() {
		ClusterLoader.endCluster();

		return "Completed disconnection";
	}
}
