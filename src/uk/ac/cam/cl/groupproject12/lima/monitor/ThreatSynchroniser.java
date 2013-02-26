package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
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

public class ThreatSynchroniser implements IDataSynchroniser {

	// The router identifier, i.e. its IP address
	private String routerID;

	/**
	 * Constructs an instance of a threat synchroniser.
	 * 
	 * @param routerID
	 *            The router ID we are to synchronise threats for (its IP
	 *            address, typically)
	 */
	public ThreatSynchroniser(String routerID) {
		this.routerID = routerID;
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
		HTable table = null;
		try {
			// Use the connection to HBase to obtain a handle on the "Threat"
			// storage table, where threat events are stored awaiting the
			// monitor's attention.
			table = new HTable(monitor.getHBaseConfig(), "Threat");

			// Filter the case based on the router ID
			Filter routerIDFilter = new RowFilter(CompareOp.EQUAL,
					new BinaryPrefixComparator(Bytes.toBytes(this.routerID)));

			Scan scan = new Scan();
			scan.setFilter(routerIDFilter);

			ResultScanner scanner = table.getScanner(scan);

			for (Result r : scanner) {
				// System.out.println("getRow:" + Bytes.toString(r.getRow()));

				String[] keys = Bytes.toString(r.getRow()).split("\\+");

				for (String s : keys)
					System.out.println(s);
			}

			Connection c = monitor.jdbcPGSQL;

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