package uk.ac.cam.cl.groupproject12.lima.monitor;

/**
 * This class encapsulates the actions required to read in statistics from
 * HBase, aggregate them appropriately and write them out to the PostgreSQL
 * relational database for data visualisation.
 * 
 * @author Team Lima
 */
public class StatisticThread extends HBaseReaderThread {

	StatisticThread(EventMonitor monitor) {
		super(monitor);
	}

	@Override
	public void run() {

	}
}
