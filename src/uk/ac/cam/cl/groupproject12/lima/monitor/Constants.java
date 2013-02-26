package uk.ac.cam.cl.groupproject12.lima.monitor;

/**
 * A location for constant values internal to the Monitor to be specified.
 * 
 * @author Team Lima
 */
public class Constants {
	public static final String ERROR_POSTGRESQL_DRIVER_MISSING = "Your PostgreSQL driver is missing!";
	public static final String ERROR_POSTGRESQL_CONFIG_NOT_ONE = "Please ensure there is exactly one <pgsql> declaration in the configuration file.";
	public static final String ERROR_POSTGRESQL_CONFIG_MALFORMED = "Please ensure all fields required are within the <pgsql> declaration.";

	public static final String PGSQL_CONNECTION_STRING = "jdbc:postgresql://%s:%i/%s";
	public static final String PGSQL_CONNECTION_XML_LOCATION = "%s.pgsqlConnectionConfig";

	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

	public static final String HBASE_KEY_SEPARATOR = "+";
}
