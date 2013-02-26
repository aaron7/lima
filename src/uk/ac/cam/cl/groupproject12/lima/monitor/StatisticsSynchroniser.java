package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;

public class StatisticsSynchroniser implements IDataSynchroniser {
    private IP routerIP;
    private int averagingPeriod = 5; //Period to average over in minutes.

    /**
     * Constructs an instance of StatisticsSynchroniser
     *
     * @param routerID The router ID we are to synchronise statistics for.
     */

    public StatisticsSynchroniser(String routerID) {
        this.routerIP = new IP(routerID);
    }
    
    /**
     * Reads data out of HBase, processes it to calculate the relevant moving averages
     * over greater periods of time than that of the logs, then writes the result into
     * PostgreSQL.
     *
     * @param monitor
     *            instance of the EventMonitor which contains the state required
     *            to interact with the various databases.
     *
     * @return A boolean indicating if the entire procedure was successful.
     * @throws SQLException
     */

	@Override
	public boolean synchroniseTables(EventMonitor monitor) throws SQLException {
		// PostgreSQL connection from the monitor.
		Connection c = monitor.jdbcPGSQL;
		
		HTable table = null;
		long currentTime = System.currentTimeMillis();
		long minimumTimestamp = currentTime - (averagingPeriod * 60000); //Timestamp corresponding to the earliest data used for averages.
		
		try {			
			// Use the connection to HBase to obtain a handle on the "Threat"
			// storage table, where threat events are stored awaiting the
			// monitor's attention.
			table = new HTable(monitor.getHBaseConfig(), "Statistic");
			
			// Filter the case based on the time processed and router ID,
			// concatenated together in the key to form its prefix. Of course,
			// the key will contain other values, so this must simply match in
			// the prefix of the key in order to obtain all fields matching on
			// these values.
			String keyPrefix = this.routerIP.getValue().toString();
			
			// The filter routerIDFilter is intended to search for all rows in
			// the database which contain the keyPrefix as their key prefix.
			Filter routerIDFilter = new RowFilter(CompareOp.EQUAL,
					new BinaryPrefixComparator(Bytes.toBytes(keyPrefix)));
			
			// Scan the database using the above filter and the required.
			Scan scan = new Scan();
			scan.setFilter();
			
		} catch (IOException e) {
			e.printStackTrace();  //TODO
		} finally {
			try {
				if (table != null)
					table.close();
			} catch (IOException e) {
				e.printStackTrace(); //TODO
			}
		}
		
		return false;
	}

}
