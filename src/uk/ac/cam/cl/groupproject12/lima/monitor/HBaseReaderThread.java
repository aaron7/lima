package uk.ac.cam.cl.groupproject12.lima.monitor;

/**
 * A generic supertype which defines the threads for reading data from HBase. It
 * is intended that this class is extended to provide the necessary
 * functionality on a table-by-table basis.
 * 
 * @author Team Lima
 * 
 */
public abstract class HBaseReaderThread implements Runnable {
	// An instance of the Event Monitor
	private final EventMonitor monitor;

	// The path of the timestamp file used to track the time of the last
	// datapoint the monitor processed for a particular table.
	private final String timestampFile;

	/**
	 * Constructor. Takes a handle to an instance of an event monitor.
	 * 
	 * @param e
	 *            An instance of an EventMonitor
	 */
	HBaseReaderThread(EventMonitor e, String timestampFile) {
		this.monitor = e;
		this.timestampFile = timestampFile;
	}

	// TODO - suitable return type
	protected static  getTimestamp(String filepath) {
		// TODO
	}

	/**
	 * Updates the timestamp used to track the last data read out for a
	 * particular table (on disk so that it persists between program launches)
	 * to a particular value of the last data point processed.
	 * 
	 * @param filepath
	 */
	protected static void updateTimestamp(String filepath, (type?) timestamp) {
		// TODO
	}
}
