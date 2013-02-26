package uk.ac.cam.cl.groupproject12.lima.monitor.database;

/**
 * Stores information required to invoke a connection to HBase.
 * 
 * @author Team Lima
 */
public class HBaseConnectionDetails {

	private String host;
	private int port;

	/**
	 * Construct a new HBaseConnectionDetails with the information about
	 * connecting to HBase.
	 * 
	 * @param host  Hostname of the HBase server.
	 * @param port  Port number on which HBase is listening.
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
