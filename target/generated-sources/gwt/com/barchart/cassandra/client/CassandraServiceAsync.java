package com.barchart.cassandra.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.ServiceDefTarget;

public interface CassandraServiceAsync
{

    /**
     * GWT-RPC service  asynchronous (client-side) interface
     * @see com.barchart.cassandra.client.CassandraService
     */
    void greetServer( java.lang.String name, AsyncCallback<java.lang.String> callback );


    /**
     * Utility class to get the RPC Async interface from client-side code
     */
    public static final class Util 
    { 
        private static CassandraServiceAsync instance;

        public static final CassandraServiceAsync getInstance()
        {
            if ( instance == null )
            {
                instance = (CassandraServiceAsync) GWT.create( CassandraService.class );
            }
            return instance;
        }

        private Util()
        {
            // Utility class should not be instanciated
        }
    }
}
