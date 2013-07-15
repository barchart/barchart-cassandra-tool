package com.barchart.cassandra.client;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("greet")
public interface CassandraService extends RemoteService {
	String connect(String name);
	String disconnect();
	String createSchema();
	String batchInsertUsers(Integer number, Integer batchNum);
	String batchModifyUsers(Integer number, Integer batchNum);
}
