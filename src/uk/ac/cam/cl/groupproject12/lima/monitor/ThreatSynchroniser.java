package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
import uk.ac.cam.cl.groupproject12.lima.hbase.HBaseAutoWriter;
import uk.ac.cam.cl.groupproject12.lima.hbase.Threat;

public class ThreatSynchroniser implements IDataSynchroniser {

	// The router identifier, i.e. its IP address
	private IP routerIP;

	// An internal handle to the EventMonitor
	private EventMonitor monitor;

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

		this.monitor = monitor;

		// PostgreSQL connection from the monitor.
		Connection c = monitor.jdbcPGSQL;
		
		// Filter the case based on the time processed and router ID,
		// concatenated together in the key to form its prefix. Of course,
		// the key will contain other values, so this must simply match in
		// the prefix of the key in order to obtain all fields matching on
		// these values.
		String keyPrefix = String
				.format(Constants.HBASE_THREAT_KEY_PREFIX,
						this.timeProcessed,
						uk.ac.cam.cl.groupproject12.lima.hbase.Constants.HBASE_KEY_SEPARATOR,
						this.routerIP.toString());

		String stmt = "INSERT INTO MESSAGES(eventID, routerIP, ip, type, status, message, createTS) VALUES (?,?,?,?,?,?,?)";
		PreparedStatement ps = c.prepareStatement(stmt);
		/*
		 * try { ps.setInt(1, 0); ps.setInt(2, 0); ps.setInt(3, 0);
		 * ps.setString(4, ""); ps.setString(5, ""); ps.setString(6, "");
		 * ps.setLong(7, 0L);
		 * 
		 * ps.executeUpdate(); } finally { ps.close(); }
		 */

		return false;
	}

	// Get the results for the key provided out of HBase and construct Threat
	// objects to internally represent them
	private List<Threat> getThreatsByKey(String keyPrefix) throws IOException {

		// List for returning results
		List<Threat> threats = new ArrayList<Threat>();

		// Use the connection to HBase to obtain a handle on the "Threat"
		// storage table, where threat events are stored awaiting the
		// monitor's attention.
		HTable table = new HTable(monitor.getHBaseConfig(), "Threat");

		// The filter routerIDFilter is intended to search for all rows in
		// the database which contain the keyPrefix as their key prefix.
		Filter routerIDFilter = new RowFilter(CompareOp.EQUAL,
				new BinaryPrefixComparator(Bytes.toBytes(keyPrefix)));

		// Scan the database using the filter specified above.
		Scan scan = new Scan();
		scan.setFilter(routerIDFilter);
		ResultScanner scanner = table.getScanner(scan);

		for (Result r : scanner) {

			// From the byte array of the resulting key, use the AutoWriter
			// to obtain an object of type Threat with all the fields for
			// this result properly instantiated with the values from HBase.
			byte[] key = r.getRow();

			// Ask the AutoWriter to get the values for the provided key and
			// populate them into the class "Threat".
			Threat t = HBaseAutoWriter.get(Threat.class, key);

			threats.add(t);
		}

		return threats;
	}
}