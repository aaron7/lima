package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;
import uk.ac.cam.cl.groupproject12.lima.hbase.Threat;

public class ThreatSynchroniser implements IDataSynchroniser {

	// The router identifier, i.e. its IP address
	private IP routerIP;

	// The time of events which this instance of the ThreatSynchroniser is
	// expected to synchronise between HBase and PGSQL (this is used to filter
	// on the basis of the key).
	private long timeProcessed;

	/**
	 * Constructs an instance of a threat synchroniser.
	 * 
	 * @param routerID
	 *            The router ID we are to synchronise threats for (its IP
	 *            address, typically)
	 */
	public ThreatSynchroniser(String routerIP, long timeProcessed) {
		this.routerIP = new IP(routerIP);
		this.timeProcessed = timeProcessed;
	}

	/**
	 * Reads threats directly from HBase, pushing them into PostgreSQL for
	 * further visualisation via the web UI.
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
		try {
			// Use the connection to HBase to obtain a handle on the "Threat"
			// storage table, where threat events are stored awaiting the
			// monitor's attention.
			table = new HTable(monitor.getHBaseConfig(), "Threat");

			// Filter the case based on the time processed and router ID,
			// concatenated together in the key to form its prefix. Of course,
			// the key will contain other values, so this must simply match in
			// the prefix of the key in order to obtain all fields matching on
			// these values.
			String keyPrefix = String
					.format(Constants.HBASE_THREAT_KEY_PREFIX,
							this.timeProcessed,
							uk.ac.cam.cl.groupproject12.lima.hbase.Constants.HBASE_KEY_SEPARATOR,
							this.routerIP.getValue().toString());

			// The filter routerIDFilter is intended to search for all rows in
			// the database which contain the keyPrefix as their key prefix.
			Filter routerIDFilter = new RowFilter(CompareOp.EQUAL,
					new BinaryPrefixComparator(Bytes.toBytes(keyPrefix)));

			// Scan the database using the filter specified above.
			Scan scan = new Scan();
			scan.setFilter(routerIDFilter);
			ResultScanner scanner = table.getScanner(scan);

			for (Result r : scanner) {

				// For each result, we obtain a full key including all the
				// values we did not know. This is used to pull the data into
				// our the "Threat" class, which represents the manner in which
				// data is stored in HBase. It is necessary to split on the
				// basis of the key separator stored externally in the
				// constants, for which we need to properly escape any special
				// characters in that constant.
				String[] keys = Bytes
						.toString(r.getRow())
						.split(Pattern
								.quote(uk.ac.cam.cl.groupproject12.lima.hbase.Constants.HBASE_KEY_SEPARATOR));

				// Pass the individual keys to the Threat class in order for it
				// to populate the values.
				Threat t = new Threat(new LongWritable(this.timeProcessed),
						this.routerIP, EventType.valueOf(keys[2]),
						new LongWritable(Long.parseLong(keys[3])));
				
				

			}

			

			String stmt = "INSERT INTO MESSAGES(eventID, routerIP, ip, type, status, message, createTS) VALUES (?,?,?,?,?,?,?)";
			PreparedStatement ps = c.prepareStatement(stmt);
			/*
			 * try { ps.setInt(1, 0); ps.setInt(2, 0); ps.setInt(3, 0);
			 * ps.setString(4, ""); ps.setString(5, ""); ps.setString(6, "");
			 * ps.setLong(7, 0L);
			 * 
			 * ps.executeUpdate(); } finally { ps.close(); }
			 */

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (table != null)
					table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return false;
	}
}