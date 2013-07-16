package com.barchart.cassandra.client;

import com.google.code.p.gwtchismes.client.GWTCAlert;
import com.google.code.p.gwtchismes.client.GWTCBox;
import com.google.code.p.gwtchismes.client.GWTCButton;
import com.google.code.p.gwtchismes.client.GWTCPopupBox;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.DockPanel;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class manager implements EntryPoint {

	/**
	 * Create a remote service proxy to talk to the server-side Greeting
	 * service.
	 */
	private final CassandraServiceAsync rpcService = GWT
			.create(CassandraService.class);

	private GWTCAlert alert = new GWTCAlert( GWTCAlert.OPTION_ROUNDED_BLUE | GWTCAlert.OPTION_ANIMATION  );

	/**
	 * This is the entry point method.
	 */
	public void onModuleLoad() {

		final GWTCBox mainPanel = new GWTCBox( );

		mainPanel.setTitle( "Cassandra tools" );

		final FlexTable functions = new FlexTable();
		functions.setStyleName( "flexPanel" );

		mainPanel.add( functions, DockPanel.NORTH );

		final GWTCButton connectBtn = new GWTCButton( GWTCButton.BUTTON_TYPE_1, "Connect to cluster" );
		connectBtn.setTitle( "Click here to establish a connection to a datacenter" );
		functions.setWidget( 0, 0, connectBtn );

		final GWTCButton rebuildBtn = new GWTCButton( GWTCButton.BUTTON_TYPE_1, "Build schema" );
		rebuildBtn.setTitle( "Click here to (re)build the schema" );
		rebuildBtn.addClickHandler( new ClickHandler() {

			@Override
			public void onClick(ClickEvent event) {

				if ( Window.confirm( "Do you really want to proceed? Data will be erased" ) )
					rpcService.createSchema( new AsyncCallback<String>() {
	
						@Override
						public void onFailure(Throwable caught) {
							alert.alert( "RPC Failure" );
						}
	
						@Override
						public void onSuccess(String result) {
							alert.alert( "Response: " + result );
						}} );
			}} );

		final GWTCButton testUserBtn = new GWTCButton( GWTCButton.BUTTON_TYPE_1, "create users" );
		testUserBtn.setTitle( "Click here to add test users to the data center and benchmark it" );
		testUserBtn.addClickHandler( new ClickHandler() {

			@Override
			public void onClick(ClickEvent event) {

				final GWTCPopupBox queryBox = new GWTCPopupBox( GWTCPopupBox.OPTION_ROUNDED_BLUE );
				final VerticalPanel panel = new VerticalPanel();
				queryBox.add( panel );

				panel.add( new Label( "Number of users" ) );
				
				final TextBox numberField = new TextBox();
				panel.add( numberField );

				panel.add( new Label( "Batch amount" ) );

				final TextBox batchNumber = new TextBox();
				panel.add( batchNumber );

				final GWTCButton connectButton = new GWTCButton( GWTCButton.BUTTON_TYPE_1, "Start" );
				panel.add( connectButton );

				connectButton.addClickHandler( new ClickHandler() {

					@Override
					public void onClick(ClickEvent event) {

						if ( numberField.getText().length() == 0 || batchNumber.getText().length() == 0 ) {
							alert.alert( "Please enter valid numbers" );
							return;
						}

						final int batchAmount = Integer.parseInt( batchNumber.getText() );
						int numberOfUsers = Integer.parseInt( numberField.getText() );

						if ( batchAmount > numberOfUsers )
							numberOfUsers = batchAmount;

						rpcService.batchInsertUsers( numberOfUsers, batchAmount,
								new AsyncCallback<String>() {

									public void onFailure(Throwable caught) {
										alert.alert( "RPC Failure" );
									}

									public void onSuccess(String result) {
										alert.alert( "Response: " + result );
									}
								});
						}} );

				queryBox.center();
			}} );

		final GWTCButton modifyUserBtn = new GWTCButton( GWTCButton.BUTTON_TYPE_1, "modify users" );
		modifyUserBtn.setTitle( "Click here to modify test users currently in the data center and benchmark it" );
		modifyUserBtn.addClickHandler( new ClickHandler() {

			@Override
			public void onClick(ClickEvent event) {

				final GWTCPopupBox queryBox = new GWTCPopupBox( GWTCPopupBox.OPTION_ROUNDED_BLUE );
				final VerticalPanel panel = new VerticalPanel();
				queryBox.add( panel );

				panel.add( new Label( "Number of users" ) );
				
				final TextBox numberField = new TextBox();
				panel.add( numberField );

				panel.add( new Label( "Batch amount" ) );

				final TextBox batchNumber = new TextBox();
				panel.add( batchNumber );

				final GWTCButton connectButton = new GWTCButton( GWTCButton.BUTTON_TYPE_1, "Start" );
				panel.add( connectButton );

				connectButton.addClickHandler( new ClickHandler() {

					@Override
					public void onClick(ClickEvent event) {

						if ( numberField.getText().length() == 0 || batchNumber.getText().length() == 0 ) {
							alert.alert( "Please enter valid numbers" );
							return;
						}

						final int batchAmount = Integer.parseInt( batchNumber.getText() );
						int numberOfUsers = Integer.parseInt( numberField.getText() );

						if ( batchAmount > numberOfUsers )
							numberOfUsers = batchAmount;

						rpcService.batchInsertUsers( numberOfUsers, batchAmount,
								new AsyncCallback<String>() {

									public void onFailure(Throwable caught) {
										alert.alert( "RPC Failure" );
									}

									public void onSuccess(String result) {
										alert.alert( "Response: " + result );
									}
								});
						}} );

				queryBox.center();
			}} );

		final GWTCButton disconnectBtn = new GWTCButton();
		disconnectBtn.setType( GWTCButton.BUTTON_TYPE_1 );
		disconnectBtn.setTitle( "Click here to disconnect from a datacenter" );
		disconnectBtn.addClickHandler( new ClickHandler() {

			@Override
			public void onClick(ClickEvent arg0) {
				rpcService.disconnect( new AsyncCallback<String>() {

					public void onFailure(Throwable caught) {
						alert.alert( "RPC Failure" );
					}

					public void onSuccess(String result) {
						alert.alert( "Response: " + result );

						functions.clear();
						functions.setWidget( 0, 0, connectBtn );
					}
				});
			}} );

		connectBtn.addClickHandler( new ClickHandler() {

			@Override
			public void onClick(ClickEvent event) {

				final GWTCPopupBox queryBox = new GWTCPopupBox( GWTCPopupBox.OPTION_ROUNDED_BLUE );
				final VerticalPanel panel = new VerticalPanel();
				queryBox.add( panel );

				panel.add( new Label( "seed" ) );
				final TextBox seedField = new TextBox();
				seedField.setText("127.0.0.1");
				seedField.setWidth( "" + Window.getClientWidth() / 8 + "px" );
				panel.add( seedField );

				panel.add( new Label( "cluster" ) );
				final TextBox clusterField = new TextBox();
				clusterField.setText("Test Cluster");
				clusterField.setWidth( "" + Window.getClientWidth() / 8 + "px" );
				panel.add( clusterField );

				// Focus the cursor on the name field when the app loads
				seedField.setFocus(true);
				seedField.selectAll();

				final GWTCButton connectButton = new GWTCButton( GWTCButton.BUTTON_TYPE_1, "Connect" );
				panel.add( connectButton );

				connectButton.addClickHandler( new ClickHandler() {

					@Override
					public void onClick(ClickEvent event) {

						if ( seedField.getText().length() == 0 ) {
							alert.alert( "Please enter a host" );
							return;
						}

						rpcService.connect(seedField.getText(), clusterField.getText(),
							new AsyncCallback<String>() {

								public void onFailure(Throwable caught) {
									alert.alert( "RPC Failure" );
								}

								public void onSuccess(String result) {
									alert.alert( "Response: " + result );

									if ( result.indexOf( "SUCCESS" ) != -1 ) {

										connectBtn.removeFromParent();
										queryBox.hide();

										disconnectBtn.setText(  "Disconnect from " + seedField.getText() );

										functions.setWidget( 0, 0, disconnectBtn );
										functions.setWidget( 1, 0, rebuildBtn );
										functions.setWidget( 2, 0, testUserBtn );
										functions.setWidget( 3, 0, modifyUserBtn );
									}
								}
							});
						}} );

				queryBox.center();
			}} );

		RootPanel.get().add( mainPanel );
	}
}
