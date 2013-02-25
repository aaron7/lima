package uk.ac.cam.cl.groupproject12.lima.cleaner;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/** The Cleaner class is used to clean the Statistics and Events tables of the HBase instance of data older than a certain threshold.
 * 
 * @author Alex Marshall
 *
 */
public abstract class Cleaner {

	/** The period of data to be retained in days.  Hard coded to a given value, but may be altered in the code.
	 * 
	 */
	private static int period = 14; //Retained period in days
	/** The timestamp corresponding to the period of data to be retained.
	 * 
	 */
	private static long minTimeStamp = System.currentTimeMillis() - (period * 86400000); //Minimum retained timestamp

	private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

	/** The main method calls the cleanTable method on both the Statistics and Events tables.
	 * @param args0 Arguments passed to the main method will not be used in execution.
	 */
	public static void main(String[] args0) {
		Configuration conf = HBaseConfiguration.create(); 
		conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "localhost");
		conf.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 2182);

		cleanTable(conf, "statistics");
		cleanTable(conf, "events");
	}

	/** The cleanTable method creates an HTable object using the passed arguments and then runs a scan of the table to be cleaned and populates a list of row to be deleted.  These are then deleted from the table and the table is closed.
	 * @param conf The Configuration of the HBase instance which will be cleaned.
	 * @param tableName The name of the table in HBase which will be cleaned.
	 */
	private static void cleanTable(Configuration conf, String tableName) {
		try {
			HTable table = new HTable(conf, tableName);
			Scan scan = new Scan();
			scan.setTimeRange(0, minTimeStamp);
			ResultScanner rs = table.getScanner(scan);
			ArrayList<Delete> deletes = new ArrayList<Delete>();
			Result r;
			try {
				for (r = rs.next(); r != null; r =rs.next()) {
					deletes.add(new Delete(r.getRow()));
				}
			}
			finally {
				rs.close();
			}
			table.delete(deletes);
			table.close();			
		} catch (IOException e) {
			throw new RuntimeException("Error on cleaning table: " + tableName, e);
		}
	}
}