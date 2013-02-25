package uk.ac.cam.cl.groupproject12.lima.monitor;

/**
 * This class encapsulates the actions required to read in threats from HBase
 * and write them out to the PostgreSQL relational database for data
 * visualisation.
 * 
 * @author Team Lima
 */
public class ThreatThread extends HBaseReaderThread {

	ThreatThread(EventMonitor monitor) {
		super(monitor);
	}

	@Override
	public void run() {

	}
}
