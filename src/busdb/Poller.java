package busdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

public class Poller {

	private String url;
	private int timeout;
	private boolean is_init;
	
	public Poller() {
		this.is_init = false;
	}
	
	public boolean isRunning() {
		return this.is_init;
	}
	
	public void setUrl( String url ) {
		this.url = url;
		this.is_init = true;
	}
	
	public void setTimeout( String timeout ) {
		this.timeout = Integer.parseInt( timeout );
	}
	
	public JSONObject getJSON() {
	    try {
	        URL u = new URL(this.url);
	        HttpURLConnection c = (HttpURLConnection) u.openConnection();
	        c.setRequestMethod("GET");
	        c.setRequestProperty("Content-length", "0");
	        c.setUseCaches(false);
	        c.setAllowUserInteraction(false);
	        c.setConnectTimeout(this.timeout);
	        c.setReadTimeout(this.timeout);
	        c.connect();
	        
	        int status = c.getResponseCode();

	        switch (status) {
	            case 200:
	            case 201:
	                BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream()));
	                StringBuilder sb = new StringBuilder();
	                String line;
	                
	                while ((line = br.readLine()) != null) {
	                    sb.append(line+"\n");
	                }
	                
	                br.close();
	                
	                JSONObject json = (JSONObject) JSONSerializer.toJSON( sb.toString() );
	                
	                return json;
	        }

	    } catch (MalformedURLException ex) {
	    	System.out.println( "BusDB:Poller : Invalid URL.." );
	        Logger.getLogger("BusDB:Poller").log(Level.SEVERE, null, ex);
	    } catch (IOException ex) {
	    	System.out.println( "BusDB:Poller : Unknown exception in data retrieval.." );
	        Logger.getLogger("BusDB:Poller").log(Level.SEVERE, null, ex);
	    }
	    return null;
	}
}
