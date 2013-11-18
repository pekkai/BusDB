package busdb;

import java.io.FileWriter;
import java.io.IOException;

public final class LogWriter {
	private static FileWriter fw;
	private static boolean is_silent;
	
	public LogWriter() {
		
	}
	
	public static void init( String file, int purge, int silent ) {
		try
		{
			boolean append = false;
			if( purge == 0)
				append = true;
			else if( purge == 1)
				append = false;
			
			if( silent == 0)
				is_silent = false;
			else if( silent == 1 )
				is_silent = true;
			
		    LogWriter.fw = new FileWriter(file, append );
		}
		catch(IOException e)
		{
			System.out.println("ERROR");
		    System.out.println("IOException: " + e.getMessage());
		}		
	}
	
	public static void write( String message ) {
		try {
			LogWriter.fw.write( message + "\n\r" );
			
			fw.flush();
			
			if( !LogWriter.is_silent )
				System.out.println( message );
			
		} catch (IOException e) {
			System.out.println( "Core:Logger : Error writing to file." );
			e.printStackTrace();
		}
	}
	
	public void shutdown() {
		try {
			LogWriter.fw.close();
		} catch (IOException e) {
			System.out.println( "Core:Logger : Error closing the file." );
			e.printStackTrace();
		}
	}
}
