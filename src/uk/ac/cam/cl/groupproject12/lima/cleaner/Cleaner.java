package uk.ac.cam.cl.groupproject12.lima.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The Cleaner class is used to clean the Statistics and Threats tables of the HBase instance of data older than a certain threshold.
 *
 * @author Team Lima
 */
public abstract class Cleaner {

    /**
     * The period of data to be retained in days.  Hard coded to a given value, but may be altered in the code.
     */
	protected static final int period = 14;

    /**
     * The timestamp corresponding to the earliest data to be retained.
     */
	protected static final long minTimeStamp = System.currentTimeMillis() - (period * 86400000);

    private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    
    private Cleaner() {
    	// Constructor not intended for use.
    }

    /**
     * The main method calls the cleanTable method on both the Statistics and Threats tables.
     *
     * @param args Arguments passed to the main method will not be used in execution.
     */
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "localhost");
        conf.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 2182);

        cleanTable(conf, "Statistic");
        cleanTable(conf, "Threat");
    }

    /**
     * The cleanTable method creates an HTable object using the passed arguments and then runs a scan of the table to be cleaned and populates a list of row to be deleted.  These are then deleted from the table and the table is closed.
     *
     * @param conf      The Configuration of the HBase instance which will be cleaned.
     * @param tableName The name of the table in HBase which will be cleaned.
     */
    protected static void cleanTable(Configuration conf, String tableName) {
        try {
            HTable table = new HTable(conf, tableName);
            Scan scan = new Scan();
            scan.setTimeRange(0, minTimeStamp);
            ResultScanner rs = table.getScanner(scan);
            ArrayList<Delete> deletes = new ArrayList<Delete>();
            Result r;
            try {
                while ((r = rs.next()) != null) {
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