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

class ThreatSynchroniser implements IDataSynchroniser {

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

		List<Threat> threats = null;
		try {
			threats = getThreatsByKey(keyPrefix);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (Threat t : threats) {

			PreparedStatement ps = null;
			try {
				// PGSQL column references need to be surrounded with double
				// quotes
				// if they contain mixed case!
				String stmt = "INSERT INTO event (\"routerIP\", type, status, message, \"createTS\", \"startTime\", \"endTime\") values (?,?,?,?,?,?,?)";
				ps = c.prepareStatement(stmt);

				ps.setString(1, t.getRouterId().getValue().toString());
				ps.setString(2, t.getType().toString());
				ps.setString(3, EventStatus.event_open.toString());
				ps.setString(4, "");
				ps.setLong(5, t.getTimeProcessed().get());
				ps.setLong(6, t.getStartTime().get());
				ps.setLong(7, t.getEndTime().get());

				// Attempt to commit
				ps.executeUpdate();
			} finally {
				if (ps != null) {
					ps.close();
				}
			}
		}

		return false;
	}
	// Get the results for the key provided out of HBase and construct Threat
	// objects to internally represent them
	private List<Threat> getThreatsByKey(String keyPrefix) throws IOException {

		// List for returning results
		List<Threat> threats = new ArrayList<Threat>();

		HTable table = null;

		try {
			// Use the connection to HBase to obtain a handle on the "Threat"
			// storage table, where threat events are stored awaiting the
			// monitor's attention.
			table = new HTable(monitor.getHBaseConfig(), "Threat");

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
		} finally {
			if (table != null)
				table.close();
		}

		return threats;
	}
}
