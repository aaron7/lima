package uk.ac.cam.cl.groupproject12.lima.monitor;

/**
 * Stores information required to invoke a connection to PGSQL.
 * 
 * @author Team Lima
 */
public class PostgreSQLConnectionDetails {

	private String host;
	private int port;
	private String dbname;
	private String username;
	private String password;

	/**
	 * Construct a new PostgreSQLConnectionDetails with the information about
	 * connecting to PGSQL.
	 * 
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 */
	public PostgreSQLConnectionDetails(String host, int port, String dbname,
			String username, String password) {
		super();
		this.host = host;
		this.port = port;
		this.dbname = dbname;
		this.username = username;
		this.password = password;
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

	/**
	 * @return the username
	 */
	String getUsername() {
		return username;
	}

	/**
	 * @return the password
	 */
	String getPassword() {
		return password;
	}

	/**
	 * @return the dbname
	 */
	String getDbname() {
		return dbname;
	}
}
