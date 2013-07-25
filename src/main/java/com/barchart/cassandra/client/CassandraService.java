package com.barchart.cassandra.client;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("greet")
public interface CassandraService extends RemoteService {
	String connect(String seed, String cluster);
	String disconnect();
	String createSchema();
	String batchInsertUsers(Integer number, Integer batchNum);
	String batchInsertTestTables(Integer maxNumber, Integer maxBatch);
	String batchModifyUsers(Integer number, Integer batchNum);
}
