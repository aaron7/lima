package uk.ac.cam.cl.groupproject12.lima.monitor;

import org.apache.hadoop.conf.Configuration;

/**
 * This class encapsulates the actions required to read in threats from HBase
 * and write them out to the PostgreSQL relational database for data visualisation.
 *
 * @author Team Lima
 */
public class ThreatThread implements Runnable {
    // Handle to the monitor in which common state is stored.
    private EventMonitor monitor;

    ThreatThread(EventMonitor monitor)
    {
        this.monitor = monitor;
    }

    @Override
    public void run() {

    }
}
