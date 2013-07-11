package com.barchart.cassandra.client;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface CassandraServiceAsync {

	void greetServer(String name, AsyncCallback<String> callback);

}
