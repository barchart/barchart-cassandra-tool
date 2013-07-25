package com.barchart.cassandra.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;

public class AstyanaxUtils {

	@SuppressWarnings("serial")
	private static Map<String, String> properties = new HashMap<String, String>() {{
		put( "cluster", "Test Cluster" );
		put( "seeds", "ec2-107-21-142-158.compute-1.amazonaws.com" );
		put( "connection.pool.name", "TestPool" );
		put ("max.conns", "100" );
		put( "max.timeout.count", "10" );
		put( "max.conns.per.host", "10" );
		put( "connect.timeout", "60000" );
		put( "socket.timeout", "60000" );
	}};

	public static void setProperty(final String prop, final String value) {
		properties.put( prop, value );
	}

	private static ClusterLoader loader = new ClusterLoader();

	public static ClusterLoader getLoader() {
		return loader;
	}

	public static Cluster getCluster() {
		return loader.getCluster( properties );
	}

	public static boolean createKeyspace(String keyspace, String strategyClass,
			int replication, String[] zones) throws ConnectionException {

		final Map<String, String> options = new HashMap<String, String>();

		if ( strategyClass.equals( "SimpleStrategy" ) )
			options.put("replication_factor", String.valueOf(replication));

		else {
			for ( String zone : zones )
				options.put( zone, String.valueOf(replication) );
		}

		KeyspaceDefinition keyspaceDef = getCluster().makeKeyspaceDefinition();
		keyspaceDef.setName(keyspace);
		keyspaceDef.setStrategyClass(strategyClass);
		keyspaceDef.setStrategyOptions(options);
		getCluster().addKeyspace(keyspaceDef);

		return true;
	}

	public static boolean dropKeyspace(String keyspace) {
		Cluster cluster = getCluster();

		try {
			cluster.describeKeyspace(keyspace);
			cluster.dropKeyspace(keyspace);
		} catch (ConnectionException e) {
			return false;
		}

		return true;
	}

	public static boolean dropColumnFamily(String keyspace, String columnFamily) {
		try {
			getCluster().dropColumnFamily(keyspace, columnFamily);
		} catch (ConnectionException e) {
			return false;
		}

		return true;
	}

	public static Keyspace getKeyspace(String keyspace) {
		try {
			return getCluster().getKeyspace(keyspace);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}

	public static boolean createColumnFamily(String keyspaceName,
			String familyName, String comparatorType) {
		try {
			Cluster cluster = getCluster();
			ColumnFamilyDefinition def = cluster.makeColumnFamilyDefinition();
			def.setName(familyName);
			def.setComparatorType(comparatorType);
			def.setKeyspace(keyspaceName);
			cluster.addColumnFamily(def);
		} catch (ConnectionException e) {
			return false;
		}

		return true;
	}

	public static boolean createColumnFamily(String keyspaceName,
			String familyName, String comparatorType,
			List<ColumnDefinition> columnDefs) {
		try {
			Cluster cluster = getCluster();
			ColumnFamilyDefinition def = cluster.makeColumnFamilyDefinition();
			def.setName(familyName);
			def.setComparatorType(comparatorType);
			def.setKeyspace(keyspaceName);
			for (ColumnDefinition cd : columnDefs) {
				def.addColumnDefinition(cd);
			}
			cluster.addColumnFamily(def);
		} catch (ConnectionException e) {
			return false;
		}

		return true;
	}

	public static boolean createColumnFamily(
			ColumnFamilyDefinition columnFamilyDefinition) {
		try {
			getCluster().addColumnFamily(columnFamilyDefinition);
		} catch (ConnectionException e) {
			return false;
		}

		return true;
	}

	public static ColumnFamilyDefinition makeColumnFamilyDefinition() {
		return getCluster().makeColumnFamilyDefinition();
	}

	public static ColumnDefinition makeColumnDefinition() {
		return getCluster().makeColumnDefinition();
	}

	public static boolean createCounterColumnFamily(String keyspaceName,
			String familyName, String comparatorType) {
		ColumnFamilyDefinition def = getCluster().makeColumnFamilyDefinition();
		def.setName(familyName);
		def.setComparatorType(comparatorType);
		def.setKeyspace(keyspaceName);
		def.setDefaultValidationClass("CounterColumnType");
		try {
			getCluster().addColumnFamily(def);
		} catch (ConnectionException e) {
			return false;
		}

		return true;
	}
}