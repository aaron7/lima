package uk.ac.cam.cl.groupproject12.lima.monitor.database;

/**
 * An exception to be raised in the case of a misconfigured PostgreSQL connection configuration file.
 */
public class PostgreSQLConfigurationException extends Exception {

	private static final long serialVersionUID = 1L;

    /**
     * @param message A string stating the issue.
     */
	public PostgreSQLConfigurationException(String message) {
		super(message);
	}
}
