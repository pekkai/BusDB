package busdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Database {
	
	private static Connection conn;
	private Statement statement;
	private boolean is_init;
	private String currentDate;

	public Database() {
		this.is_init = false;
		
		SimpleDateFormat date_format = new SimpleDateFormat("yyyy_MM_dd");
		this.currentDate = date_format.format( Calendar.getInstance().getTime() );
	}
	
	static public Connection getInstance() {
		return Database.conn;
	}
	
	public void init( String address, String port, String username, String password, String dbname ) {
		try {
			
			Class.forName("org.postgresql.Driver");
			
		} catch (ClassNotFoundException e) {
			LogWriter.write( "BusDB:Database : No JDBC Driver found...");
			e.printStackTrace();
		}
		
		conn = null;

		
		try {
		
			conn = DriverManager.getConnection( "jdbc:postgresql://"+address+":"+Integer.parseInt(port)+"/", 
												username,
												password);

			conn.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED);
			
		} catch (SQLException e) {
			LogWriter.write( "BusDB:Database : Connection failed. Check conf.sys for credentials...");
			e.printStackTrace();
		}
		
		if( conn != null ) {
			LogWriter.write( "BusDB:Database : Connected to database.." );
			this.is_init = true;
			LogWriter.write( "BusDB:Database : Initialized successfully." );
			
			try {
				LogWriter.write( "BusDB:Database : Creating database for current date." );
				
				this.statement = Database.conn.createStatement();
				
				String sql = "CREATE DATABASE db_" + this.currentDate;
				this.statement.executeUpdate( sql );
				
				
				
				
				this.statement.close();
				Database.conn.close();
				
				
			} catch (SQLException e) {
				LogWriter.write( "BusDB:Database :    ... database already exists. ( we hope )." );				
			}
			
			
			try {
				LogWriter.write( "BusDB:Database : Connecting to current database." );
				conn = DriverManager.getConnection( "jdbc:postgresql://"+address+":"+Integer.parseInt(port)+"/db_" + this.currentDate, 
													username,
													password);

				conn.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED);
				
			} catch (SQLException e) {
				LogWriter.write( "BusDB:Database : Connection failed. Something went wrong. ");
				e.printStackTrace();
			}	
			
			try {
			
				this.statement = Database.conn.createStatement();
				
				String sql =   "CREATE TABLE catalog (" +
					    "id SERIAL," +
						"response_time_stamp INTEGER, " + 
					    "producer_ref varchar(10)," +
					    "time_of_rec timestamp )";
				
				this.statement.execute( sql );

				sql =   "CREATE TABLE busdata (" +
						"catalog_id INTEGER NOT NULL, " +
						"lineref varchar(10) NOT NULL, " +
						"directionref INTEGER, " +
						"originname varchar(30), " +
						"destinationname varchar(30), " +
						"longtitude double precision, " +
						"latitude double precision, " +
						"delay varchar(40), " + 
						"vehicleref varchar(20)," +
						"dvjref INTEGER," +
						"bearing double precision," +
						"operatorref varchar(30) )";
				this.statement.execute( sql );
				this.statement.close();
			
				LogWriter.write( "BusDB:Database : Created tables for database." );

			} catch (SQLException e) {
				LogWriter.write( "BusDB:Database : Failed to create tables for selected database" );
			}
			
			
		}
		else
			LogWriter.write( "BusDB:Database : Failed to make connection.." );
		
		
	}
	
	public int addCatalog( int response, String producer ) {
		try {
			this.statement = Database.conn.createStatement();
			
			String sql = "INSERT INTO catalog (response_time_stamp, producer_ref, time_of_rec) VALUES ( " +
						 "" + response + ", '" + producer + "', 'now')";
			
			this.statement.executeUpdate( sql, Statement.RETURN_GENERATED_KEYS );
			
			ResultSet rs = this.statement.getGeneratedKeys();
			
			if( rs.next() ) {
				return rs.getInt(1);
			}
			
		} catch( SQLException e ) {
			LogWriter.write( "BusDB:Database : Failed to add new data into catalog table. " );
			e.printStackTrace();
		}
		
		return -1;
	}
	
	public void addDataEntry( int id, String lineref, int directionref, String origin, String destination, String longitude,
			  				  String latitude, String delay, String vehicleref, int djvref, String bearing, String operatorref ) {
		
		try {
			this.statement = Database.conn.createStatement();
			
			String sql = "INSERT INTO busdata VALUES ( " +
						 "" + id + "," +
						 "'" + lineref + "', " + directionref + ", " +
						 "'" + origin + "', '" + destination + "', " +
						 "" + longitude + ", " + latitude + ", " +
						 "'" + delay + "', '" + vehicleref + "',"+
						 "" + djvref + ", '" + bearing + "', '" +
						 "" + operatorref +"')";
		
			this.statement.executeUpdate( sql );
			this.statement.close();
			
		} catch( SQLException e ) {
			LogWriter.write( "BusDB:Database : Failed to add new data into busdata table. " );
			e.printStackTrace();
		}
		
	}
	
	public boolean isRunning() {
		return this.is_init;
	}
	
	public void shutdown() {
		try {
			Database.conn.close();
		} catch (SQLException e) {
			LogWriter.write( "BusDB:Database : Shutdown with errors.." );
			LogWriter.write( "BusDB:Dataase : Exception: " + e.getMessage() );
			return;
		}
		
		LogWriter.write( "BusDB:Database : Shutting down.." );
	}
	
}
