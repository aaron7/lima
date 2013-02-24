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

public abstract class Cleaner {

	private static int period = 14; //Retained period in days
	private static long minTimeStamp = System.currentTimeMillis() - (period * 86400000); //Minimum retained timestamp

	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

	public static void main(String[] args0) {
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
			try {
				for (Result r = rs.next(); r != null; r =rs.next()) {
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