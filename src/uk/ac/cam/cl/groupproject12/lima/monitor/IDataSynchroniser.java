package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.sql.*;

/**
 * An interface to describe classes which implement the logic to replicate data from HBase to PostgreSQL.
 */
public interface IDataSynchroniser {

    /**
     * Synchronises tables from HBase to PostgreSQL in accordance with the rules for a particular implementation (one
     * implementation per table / dataset to replicate).
     *
     * @param monitor
     *         Instance of the EventMonitor which contains the state required to interact with the various databases.
     *
     * @return True on success, false on failure.
     */
    boolean synchroniseTables(EventMonitor monitor) throws SQLException;

}
