
package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import uk.ac.cam.cl.groupproject12.lima.hbase.Statistic;

/**
 * Statistics synchroniser synchronises the stats on a per-router basis from
 * HBase to PostgreSQL, aggregating them along the way. Due to the manner in
 * which the EventMonitor invokes the synchronisers, this is package visible
 * only.
 */
class StatisticsSynchroniser implements IDataSynchroniser {
	private IP routerIP;
	private int averagingPeriod = 5; // Period to average over in minutes.
	private EventMonitor monitor;

	/**
	 * Constructs an instance of StatisticsSynchroniser
	 * 
	 * @param routerID
	 *            The router ID we are to synchronise statistics for.
	 */

	public StatisticsSynchroniser(String routerID) {
		this.routerIP = new IP(routerID);
	}

	/**
	 * Reads data out of HBase, processes it to calculate the relevant moving
	 * averages over greater periods of time than that of the logs, then writes
	 * the result into PostgreSQL.
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
		String keyPrefix = this.routerIP.toString();
		long currentTime = System.currentTimeMillis();
		// Timestamp corresponding to the earliest data used for averages.
		long minimumTimestamp = currentTime - (averagingPeriod * 60000);

		List<Statistic> statistics = null;
		try {
			statistics = getStatisticsByKeyAndTimestamp(keyPrefix,
					minimumTimestamp, currentTime);
		} catch (IOException e) {
			e.printStackTrace(); // TODO
		}

		long lastSeen = 0;
		int numberOfRouters = 0;

		String stmt1 = "SELECT \"lastSeen\" FROM router WHERE \"routerIP\"= ?";
		PreparedStatement ps1 = c.prepareStatement(stmt1);
		ps1.setString(1, routerIP.getValue().toString());

		try {
			ResultSet rs = ps1.executeQuery();
			while (rs.next()) {
				lastSeen = rs.getLong("lastSeen");

				// Increment the number of routers processed -- this is an
				// important flag which will be used later when updating /
				// inserting to this table.
				numberOfRouters++;
			}

		} finally {
			if (ps1 != null) {
				ps1.close();
			}
		}

		int flowsPerPeriod = 0;
		int packetsPerPeriod = 0;
		int bytesPerPeriod = 0;

		long lastSeenTmp;
		for (Statistic s : statistics) {
			lastSeenTmp = s.getTimeFrame().get();
			if (lastSeenTmp > lastSeen) {
				lastSeen = lastSeenTmp;
			}

			flowsPerPeriod += s.getFlowCount().get();
			packetsPerPeriod += s.getPacketCount().get();
			bytesPerPeriod += s.getTotalDataSize().get();
		}

		int flowsPH = Math.round((float) flowsPerPeriod * 60f
				/ (float) averagingPeriod);
		int packetsPH = Math.round((float) packetsPerPeriod * 60f
				/ (float) averagingPeriod);
		int bytesPH = Math.round((float) bytesPerPeriod * 60f
				/ (float) averagingPeriod);

		// Check the number of routers returned earlier. This dictates whether
		// it is necessary to use INSERT INTO or an UPDATE SQL operation.
		String stmt2;
		PreparedStatement ps2;
		if (numberOfRouters > 0) {
			stmt2 = "UPDATE router SET \"lastSeen\" = ?, \"flowsPH\" = ?, \"packetsPH\" = ?, \"bytesPH\" = ? WHERE \"routerIP\" = ?";
			ps2 = c.prepareStatement(stmt2);

			// Set parameters in ps2 -- again the ordering will vary depending
			// on which SQL query is in use, and it was deemed to be simplest to
			// just repeat this block here with different orders.
			ps2.setLong(1, lastSeen);
			ps2.setInt(2, flowsPH);
			ps2.setInt(3, packetsPH);
			ps2.setInt(4, bytesPH);
			ps2.setString(5, routerIP.getValue().toString());
		} else {
			stmt2 = "INSERT INTO router (\"routerIP\", \"lastSeen\", \"flowsPH\", \"packetsPH\", \"bytesPH\") VALUES(?,?,?,?,?)";
			ps2 = c.prepareStatement(stmt2);

			ps2.setString(1, routerIP.getValue().toString());
			ps2.setLong(2, lastSeen);
			ps2.setInt(3, flowsPH);
			ps2.setInt(4, packetsPH);
			ps2.setInt(5, bytesPH);
		}

		try {
			// Do the update operation
			ps2.executeUpdate();
		} finally {
			if (ps2 != null) {
				ps2.close();
			}
		}

		return true;
	}
	// Get the results for the key prefix provided from HBase and construct
	// Statistic
	// objects to internally represent them.
	private List<Statistic> getStatisticsByKeyAndTimestamp(String keyPrefix,
			long minimumTimestamp, long currentTime) throws IOException {
		List<Statistic> statistics = new ArrayList<Statistic>();

		HTable table = null;

		try {
			// Use the connection to HBase to obtain a handle on the "Threat"
			// storage table, where threat events are stored awaiting the
			// monitor's attention.
			table = new HTable(monitor.getHBaseConfig(), "Statistic");

			// The filter routerIDFilter is intended to search for all rows in
			// the database which contain the keyPrefix as their key prefix.
			Filter routerIPFilter = new RowFilter(CompareOp.EQUAL,
					new BinaryPrefixComparator(Bytes.toBytes(keyPrefix)));

			// Scan the database using the above filter and the required.
			Scan scan = new Scan();
			scan.setFilter(routerIPFilter);
			// scan.setTimeRange(minimumTimestamp, currentTime);
			ResultScanner scanner = table.getScanner(scan);

			for (Result r : scanner) {
				// From the byte array of the resulting key, use the AutoWriter
				// to obtain an object of type Statistic with all the fields for
				// this result properly instantiated with the values from HBase.
				byte[] key = r.getRow();

				// Ask the AutoWriter to get the values for the provided key and
				// populate them into the class "Threat".
				Statistic s = HBaseAutoWriter.get(Statistic.class, key);

				statistics.add(s);
			}
		} finally {
			if (table != null)
				table.close();
		}

		return statistics;
	}

}