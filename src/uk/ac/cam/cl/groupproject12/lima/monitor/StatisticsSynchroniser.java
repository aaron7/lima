package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class StatisticsSynchroniser implements IDataSynchroniser {
    private int routerID;

    /**
     * Constructs an instance of StatisticsSynchroniser
     *
     * @param routerID The router ID we are to synchronise statistics for.
     */

    public StatisticsSynchroniser(int routerID) {
        this.routerID = routerID;
    }

    /**
     * Reads threats out of HBase, processes them to calculate the relevant moving averages over greater periods of time than that of the logs, then writes the result into PostgreSQL.
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
			table = new HTable(monitor.getBaseConfig(), "Statistic");
			
			// Row filter based on the router ID in the key. Substitutes in the
			// key separator.
			Filter routerIDFilter = new RowFilter (
					CompareFilter.CompareOp.EQUAL, new RegextringComparator(
							String.format(this.routerID + "%s",
									Constants.HBASE_KEY_SEPERATOR)));
			
			Scan scan = new Scan();
			FilterList fl = new FilterList();
			fl.addFilter(routerIDfilter);
			ResultScanner scanner = table.getScanner(scan);
			
			for (Result r : scanner) {
				; //TODO
			}
			
			Connection c = monitor.jdbcPGSQL;
			
			String stmt = ""; //TODO
			PrepareStatement ps = c.prepareStatement(stmt);
			try {

		} catch (IOException e) {
			e.printStackTrace();  //TODO
		} finally {
			try {
				if (table != null)
					table.close()
			} catch (IOException e) {
				e.printStackTrace(); //TODO
			}
		}
		
		return false;
	}

}
