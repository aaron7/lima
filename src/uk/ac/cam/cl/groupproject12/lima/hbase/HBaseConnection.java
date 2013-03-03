package uk.ac.cam.cl.groupproject12.lima.hbase;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;

public class HBaseConnection {
    Configuration conf;

    public HBaseConnection() {
        conf = HBaseConfiguration.create();
        conf.set(HBaseConstants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "localhost");
        conf.setInt(HBaseConstants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 2182);
    }

    /**
     * @return The generated configuration file for this HBaseConnection object.
     */
    public Configuration getConfig() {
        return conf;
    }

}
