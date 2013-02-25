package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Manages the replication of data between HBase and PostgreSQL on completion of
 * a Hadoop M-R job.
 * 
 * @author Team Lima
 * 
 */
public class EventMonitor {

	Configuration hbaseConfig = HBaseConfiguration.create();
	Connection jdbcPGSQL = null;

	public EventMonitor(HBaseConnectionDetails hbaseConf,
			IDataSynchroniser synchroniser) {
		hbaseConfig.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM,
				hbaseConf.getHost());
		hbaseConfig.setInt(Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT,
				hbaseConf.getPort());

		// Set up PGSQL connection
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.err.println(Constants.ERROR_POSTGRESQL_DRIVER_MISSING);
			System.exit(1);
		}

		try {
			this.jdbcPGSQL = DriverManager.getConnection(
					String.format(Constants.PGSQL_CONNECTION_STRING,
							hbaseConf.getHost(), hbaseConf.getPort(),
							hbaseConf.getDbname()), hbaseConf.getUsername(),
					hbaseConf.getPassword());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
