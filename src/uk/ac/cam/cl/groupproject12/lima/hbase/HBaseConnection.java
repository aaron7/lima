package uk.ac.cam.cl.groupproject12.lima.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseConnection {
	
	Configuration conf;
	
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
	
	
	public HBaseConnection() {
		conf = HBaseConfiguration.create();
		conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "localhost");
		conf.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 2182);
	}
	
	public Configuration getConfig(){
		return conf;
	}
	
}
