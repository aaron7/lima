package uk.ac.cam.cl.groupproject12.lima.hadoop;

import uk.ac.cam.cl.groupproject12.lima.monitor.*;
import uk.ac.cam.cl.groupproject12.lima.monitor.database.*;
import uk.ac.cam.cl.groupproject12.lima.web.*;

import java.io.*;

/**
 * This is the main Java class which is run for each router when we receive netflow data. It will be called by the
 * importer script.
 */
public class RunJobs {

    /**
     * Delegates runJobs tasks.
     *
     * @param args
     *         A single parameter of the filename of the data.  Assumed to be of the form
     *         [routerIP]-[timestampMade]-netflow.csv
     *
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String[] tokens = args[0].split("-");
        String routerIp = tokens[0];
        long timestamp = Long.valueOf(tokens[1]);
        runJobs(routerIp, timestamp);
    }

    /**
     * Issues all jobs on new threads.
     *
     * @param routerIp
     *         Router IP the jobs are for.
     * @param timestamp
     *         Timestamp of the file.
     */
    public static void runJobs(final String routerIp, final long timestamp) {
        final int jobParts = 3; // There are 3 parts:Stats,Dos1,Dos2
        // Tell the web that we have a new job
        Web.newJob(routerIp, timestamp, jobParts);

        Thread[] threads = new Thread[]{
                new StatisticsJob().getThread(routerIp, timestamp),
                new DosJob().getThread(routerIp, timestamp),
                new SingleFlowJob().getThread(routerIp, timestamp),
                new ScanningJob().getThread(routerIp, timestamp)};
        try {
            System.out.println("Starting threads...");
            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            System.out.println("... All threads finished!");
        } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected interruption exception", e);
        }

        // Event monitor now runs and calls Web.updateJob(....,true) to tell the
        // web it has completely finished
        HBaseConnectionDetails hbaseConf = HBaseConnectionDetails.getDefault();
        EventMonitor.doSynchronise(new IP(routerIp), hbaseConf, timestamp);
    }


}