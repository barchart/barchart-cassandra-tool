package com.barchart.cassandra.client;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface CassandraServiceAsync {

	void connect(String seed, String cluster, AsyncCallback<String> callback);

	void disconnect(AsyncCallback<String> callback);

	void createSchema(AsyncCallback<String> callback);

	void batchInsertUsers(Integer number, Integer batchNum,
			AsyncCallback<String> callback);

	void batchInsertTestTables(Integer maxNumber, Integer maxBatch,
			AsyncCallback<String> callback);

	void batchModifyUsers(Integer number, Integer batchNum,
			AsyncCallback<String> callback);
}
