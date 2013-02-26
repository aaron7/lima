package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;

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
	public boolean synchroniseTables(EventMonitor monitor) {

		try {
			HTable table = new HTable(monitor.getHBaseConfig(), "Threat");

			// Set up a RowFilter to filter based on router ID
			List<Filter> filters = new ArrayList<Filter>();

			Filter routerIDFilter = new RowFilter(
					CompareFilter.CompareOp.EQUAL, new RegexStringComparator(
							String.format(this.routerID + "%s",
									Constants.HBASE_KEY_SEPARATOR)));

			filters.add(routerIDFilter);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}
}
