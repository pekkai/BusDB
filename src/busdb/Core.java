package busdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class Core {
	private HashMap<String, String> config;
	private Poller poller;
	private Database db;
	private Timer timer;
	private Timer endTimer;

	public Core() {
		this.config = new HashMap<String, String>();
		this.poller = new Poller();
		this.db = new Database();
		
		this.config.put("poller_json_url", "");		
		this.config.put("poller_timeout", "1000");
		this.config.put("polling_delay", "1000");
		this.config.put("life", "1" );
		
		this.config.put("db_addr", "" );
		this.config.put("db_name", "" );
		this.config.put("db_user", "" );
		this.config.put("db_pass", "" );
		this.config.put("db_port", "" );

		this.config.put("log_file", "output.log" );
		this.config.put("log_purge", "0" );
		this.config.put("log_silent", "0" );

		
		this.timer = new Timer();
		this.endTimer = new Timer();
	}
	
	public void start() {
		this.timer.scheduleAtFixedRate( new TimerTask() {

			@Override
			public void run() {
				processJSON();
				
			}}, Long.parseLong( this.config.get("polling_delay") ), 
				Long.parseLong( this.config.get("polling_delay") ) );
		
		this.endTimer.schedule( new TimerTask() {

			@Override
			public void run() {
				shutdown();
				
			}},  Long.parseLong( this.config.get("life") ) * 1000 * 60 * 60 );		
	}
	
	public final void shutdown() {
		LogWriter.write( "BusDB:Core : Starting graceful shutdown... " );
		this.db.shutdown();
		this.timer.cancel();
		
		LogWriter.write( "BusDB:Core : Shutdown successfully. " );
		System.exit( 0 );
	}
	
	public void init( String settings ) {
		
		if( settings.isEmpty() || settings == null );
		else {
			try {
			    BufferedReader in = new BufferedReader(new FileReader(settings));
			    
			    String str;
			    
			    while ((str = in.readLine()) != null) {
			       if( str.startsWith(";") || str.isEmpty() )
			    	   continue;
			       else {	   

			    	  int idx = str.indexOf( "=" );
			    	  String key = str.substring( 0, idx );
			    	  
			    	    
			    	  int idx2 = str.lastIndexOf( "=" );
			    	  String value = str.substring( idx2+1, str.length() );
			    	  this.config.put( key.trim(), value.trim());  

			       }
			    }
			    
			    in.close();
			} 
			catch (IOException e) {
				System.out.println("BusDB:Core : Exception occured while reading configuration file." );
				System.out.println("BusDb:Core : Initialization failed.." );
				Logger.getLogger("busdb.Core").log(Level.SEVERE, null, e);
				return;
			}
			
		
		}
		
		LogWriter.init( this.config.get( "log_file"), 
						Integer.parseInt( this.config.get( "log_purge") ), 
						Integer.parseInt( this.config.get( "log_silent") ) );
		
		this.poller.setUrl( this.config.get( "poller_json_url") );
		this.poller.setTimeout( this.config.get( "poller_timeout") );

		this.db.init( this.config.get( "db_addr"),  
				 	  this.config.get( "db_port"),
					  this.config.get( "db_user"),  
					  this.config.get( "db_pass"),  
					  this.config.get( "db_name") );
		
		
		
		LogWriter.write("BusDB:Core : Core initialized successfully." );
		LogWriter.write("BusDB:Core : Running..." );

	}

	public final void processJSON() {
		if ( !this.poller.isRunning() || !this.db.isRunning() ) {
			LogWriter.write( "BusDB:Core : Core components not initialized properly.. halting polling!" );
			this.timer.cancel();
		}

		long startTime = System.currentTimeMillis();	
		
		try {
		
		JSONObject root = this.poller.getJSON();
		JSONObject sd = root.getJSONObject( "Siri" ).getJSONObject("ServiceDelivery");
		
		int response = sd.getInt( "ResponseTimestamp" );
		String producer = sd.getJSONObject( "ProducerRef").getString("value");
		
		int catalog_id = this.db.addCatalog( response, producer );
	
		
		JSONObject va = sd.getJSONArray( "VehicleMonitoringDelivery" ).getJSONObject( 0 );
		JSONArray buslist = va.getJSONArray( "VehicleActivity" );

		for( int i = 0; i < buslist.size(); i++ ) {
			JSONObject busdata = buslist.getJSONObject( i ).getJSONObject( "MonitoredVehicleJourney");

			String lineref =  busdata.getJSONObject( "LineRef").getString( "value");
			int directionref = busdata.getJSONObject( "DirectionRef").getInt( "value");
			String origin = busdata.getJSONObject( "OriginName").getString( "value");
			String destination = busdata.getJSONObject( "DestinationName").getString( "value");
			String longitude = busdata.getJSONObject( "VehicleLocation").getString( "Longitude");
			String latitude  =  busdata.getJSONObject( "VehicleLocation").getString( "Latitude");
			String delay = busdata.getString( "Delay");
			String vehicleref = busdata.getJSONObject( "VehicleRef").getString( "value");
			
			int dvjref = busdata.getJSONObject( "FramedVehicleJourneyRef" ).getInt( "DatedVehicleJourneyRef");
			String bearing = busdata.getString( "Bearing");
			String operatorref = busdata.getJSONObject( "OperatorRef").getString( "value");
			
			this.db.addDataEntry( catalog_id,
								  lineref,
								  directionref,
								  origin,
								  destination,
								  longitude,
								  latitude,
								  delay,
								  vehicleref,
								  dvjref,
								  bearing,
								  operatorref);
		
		}
		} catch( Exception e ) {
			LogWriter.write( "BusDB:Core : Error occured in data processing with message: " + e.getMessage() );
		}
		

		if( Integer.parseInt( this.config.get( "log_processes") ) == 1 ) {
			LogWriter.write( "BusDB:Core : Data processed in " + (System.currentTimeMillis() - startTime) + " ms." );						
		}
	}

	
}
