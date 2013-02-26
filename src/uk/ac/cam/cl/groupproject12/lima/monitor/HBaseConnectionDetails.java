package uk.ac.cam.cl.groupproject12.lima.monitor;

/**
 * Stores information required to invoke a connection to HBase.
 * 
 * @author Team Lima
 */
public class HBaseConnectionDetails {

	private String host;
	private int port;

	/**
	 * Construct a new PostgreSQLConnectionDetails with the information about
	 * connecting to PGSQL.
	 * 
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 */
	public HBaseConnectionDetails(String host, int port) {
		super();
		this.host = host;
		this.port = port;
	}

	/**
	 * @return the host
	 */
	String getHost() {
		return host;
	}

	/**
	 * @return the port
	 */
	int getPort() {
		return port;
	}
}
