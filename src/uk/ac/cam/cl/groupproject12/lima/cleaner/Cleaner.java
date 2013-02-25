package uk.ac.cam.cl.groupproject12.lima.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;

public abstract class Cleaner {

    private static final int period = 14; //Retained period in days
    private static final long minTimeStamp = System.currentTimeMillis() - (period * 86400000); //Minimum retained timestamp

    public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "localhost");
        conf.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 2182);

        cleanTable(conf, "statistics");
        cleanTable(conf, "events");
    }

    private static void cleanTable(Configuration conf, String tableName) {
        try {
            HTable table = new HTable(conf, tableName);
            Scan scan = new Scan();
            scan.setTimeRange(0, minTimeStamp);
            ResultScanner rs = table.getScanner(scan);
            ArrayList<Delete> deletes = new ArrayList<Delete>();
            Result r;
            try {
                while((r = rs.next()) != null) {
                    deletes.add(new Delete(r.getRow()));
                }
            } finally {
                rs.close();
            }
            table.delete(deletes);
            table.close();
        } catch (IOException e) {
            throw new RuntimeException("Error on cleaning table: " + tableName, e);
        }
    }
}