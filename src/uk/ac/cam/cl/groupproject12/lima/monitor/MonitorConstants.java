package uk.ac.cam.cl.groupproject12.lima.monitor;

/**
 * A location for constant values internal to the Monitor to be specified.
 * 
 * @author Team Lima
 */
public class MonitorConstants {
    /**
     * String to display when no PostgreSQL driver is found.
     */
	public static final String ERROR_POSTGRESQL_DRIVER_MISSING = "Your PostgreSQL driver is missing!";
    /**
     * String to display when the amount of results from the XML file is not exactly 1.
     */
	public static final String ERROR_POSTGRESQL_CONFIG_NOT_ONE = "Please ensure there is exactly one <pgsql> declaration in the configuration file.";
    /**
     * String to display if the result is missing arguments.
     */
    public static final String ERROR_POSTGRESQL_CONFIG_MALFORMED = "Please ensure all fields required are within the <pgsql> declaration.";

    /**
     * Connection string, for substitution with parameters in the code.
     */
	public static final String PGSQL_CONNECTION_STRING = "jdbc:postgresql://%s:%d/%s";
    /**
     * Filepath string to the configuration file.
     */
	public static final String PGSQL_CONNECTION_XML_LOCATION = "%s/.pgsqlConnectionConfig";

    /**
     * Substitution string to simplify the concatenation of two keys and a key separator.
     */
	public static final String HBASE_CONCATENATE_TWO_KEYS = "%s%s%s";
}
