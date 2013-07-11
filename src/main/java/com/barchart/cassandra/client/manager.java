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
		mainPanel.setSize( "" + Window.getClientWidth() + "px", "" + Window.getClientHeight() + "px" );

		mainPanel.setTitle( "Cassandra tools" );

		final VerticalPanel functions = new VerticalPanel();
		mainPanel.add( functions, DockPanel.CENTER );

		final GWTCButton connectBtn = new GWTCButton( GWTCButton.BUTTON_TYPE_1, "Connect to cluster" );
		connectBtn.addClickHandler( new ClickHandler() {

			@Override
			public void onClick(ClickEvent event) {

				final GWTCPopupBox queryBox = new GWTCPopupBox( GWTCPopupBox.OPTION_ROUNDED_BLUE );
				final HorizontalPanel panel = new HorizontalPanel();
				queryBox.add( panel );

				final TextBox nameField = new TextBox();
				nameField.setText("Name");
				nameField.setWidth( "" + Window.getClientWidth() / 4 + "px" );
				panel.add( nameField );

				// Focus the cursor on the name field when the app loads
				nameField.setFocus(true);
				nameField.selectAll();

				final GWTCButton connectButton = new GWTCButton( GWTCButton.BUTTON_TYPE_1, "Connect" );
				panel.add( connectButton );

				connectButton.addClickHandler( new ClickHandler() {

					@Override
					public void onClick(ClickEvent event) {

						if ( nameField.getText().length() == 0 ) {
							alert.alert( "Please enter a host" );
							return;
						}

						final String host = nameField.getText();

						rpcService.greetServer(host,
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

		functions.add( connectBtn );

		final GWTCButton testUserBtn = new GWTCButton( GWTCButton.BUTTON_TYPE_1, "create users" );
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

						final int numberOfUsers = Integer.parseInt( numberField.getText() );
						final int batchAmount = Integer.parseInt( batchNumber.getText() );

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

		functions.add( testUserBtn );
		RootPanel.get().add( mainPanel );
	}
}
