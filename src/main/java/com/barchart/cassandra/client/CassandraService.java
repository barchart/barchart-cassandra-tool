package com.barchart.cassandra.client;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("greet")
public interface CassandraService extends RemoteService {
	String greetServer(String name) throws IllegalArgumentException;

	String batchInsertUsers(Integer number, Integer batchNum) throws IllegalArgumentException;

}
