package uk.ac.cam.cl.groupproject12.lima.monitor;

import org.apache.hadoop.conf.Configuration;

/**
 * This class encapsulates the actions required to read in statistics from HBase,
 * aggregate them appropriately and write them out to the PostgreSQL relational
 * database for data visualisation.
 *
 * @author Team Lima
 */
public class StatisticThread implements Runnable {
    // Handle to the monitor in which common state is stored.
    private EventMonitor monitor;

    StatisticThread(EventMonitor monitor)
    {
        this.monitor = monitor;
    }

    @Override
    public void run() {
        //TODO
    }
}
