package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class StatisticsSynchroniser implements IDataSynchroniser {
    private String routerID;
    private int averageingPeriod = 5; //Period to average over in minutes.

    /**
     * Constructs an instance of StatisticsSynchroniser
     *
     * @param routerID The router ID we are to synchronise statistics for.
     */

    public StatisticsSynchroniser(int routerID) {
        this.routerID = routerID;
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
		HTable table = null;
		long currentTime = System.currentTimeMillis();
		long minimumTimestamp = currentTime - (averagingPeriod * 60000); //Timestamp corresponding to the earliest data used for averages.
		try {
			// Use the connection to HBase to obtain a handle on the "Statistic"
			// storage table, where router data is stored awaiting the
			// monitor's attention.
			table = new HTable(monitor.getBaseConfig(), "Statistic");
			
			// Filter the case based on the router ID
			Filter routerIDFilter = new RowFilter (CompareOp.EQUAL,
					new BinaryPrefixComparator(Bytes.toBytes(this.routerID)))));
			
			Scan scan = new Scan();
			scan.setFilter(routerIDFilter);
			scan.setTimeRange(minimumTimestamp, currentTime);
			
			ResultScanner scanner = table.getScanner(scan);
			
			long flowsPH = 0;
			long packetsPH = 0;
			long bytesPH = 0;
			
			for (Result r : scanner) {
				; //TODO
			}
			
			Connection c = monitor.jdbcPGSQL;
			
			String stmt = "INSERT INTO Router(routerIP, lastSeen, flowsPH, packetsPH, bytesPH) VALUES (?,?,?,?,?)"; //TODO
			PrepareStatement ps = c.prepareStatement(stmt);
			

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
