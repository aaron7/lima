package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

public class ThreatSynchroniser implements IDataSynchroniser {
    private int routerID;

    /**
     * Constructs an instance of a threat synchroniser.
     *
     * @param routerID
     *            The router ID we are to synchronise threats for.
     */
    public ThreatSynchroniser(int routerID) {
        this.routerID = routerID;
    }

    @Override
    public boolean synchroniseTables(EventMonitor monitor) throws SQLException {

        try {
            HTable table = new HTable(monitor.getHbaseConfig(), "Threat");

            // Set up a RowFilter to filter based on router ID
            List<Filter> filters = new ArrayList<Filter>();

            Filter routerIDFilter = new SingleColumnValueFilter();

            Connection c = monitor.jdbcPGSQL;

            String stmt = "INSERT INTO MESSAGES(eventID, routerIP, ip, type, status, message, createTS) VALUES (?,?,?)";
            PreparedStatement ps = c.prepareStatement(stmt);
            try {
                ps.setInt(1, 0);
                ps.setInt(2, 0);
                ps.setInt(3, 0);
                ps.setString(4, "");
                ps.setString(5, "");
                ps.setString(6, "");
                ps.setLong(7, 0L);

                ps.executeUpdate();
            } finally {
                ps.close();
            }


        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return false;
    }
}