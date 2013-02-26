package uk.ac.cam.cl.groupproject12.lima.monitor;

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
		// TODO Auto-generated method stub
		return false;
	}

}
