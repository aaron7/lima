package uk.ac.cam.cl.groupproject12.lima.monitor;

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

	public EventMonitor() {
		hbaseConfig.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM,
				"localhost");
		hbaseConfig.setInt(Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT,
				2182);
	}

}
