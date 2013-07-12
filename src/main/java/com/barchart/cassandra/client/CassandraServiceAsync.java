package com.barchart.cassandra.client;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface CassandraServiceAsync {

	void connect(String name, AsyncCallback<String> callback);

	void batchInsertUsers(Integer number, Integer batchNum,
			AsyncCallback<String> callback);

	void createSchema(AsyncCallback<String> callback);

}
