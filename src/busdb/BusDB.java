package busdb;

public class BusDB {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String config = "";
		
		for( int i = 0; i < args.length; i+=2 ) {
		
			if( args[i].equals( "-conf" ) )
				config = args[i+1];
		}
		
		
		Core core = new Core();
		core.init(config);
		

		core.start();
		return;
	}

}
