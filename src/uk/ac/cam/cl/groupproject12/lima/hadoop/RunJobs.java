package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.IOException;

import uk.ac.cam.cl.groupproject12.lima.monitor.EventMonitor;
import uk.ac.cam.cl.groupproject12.lima.monitor.database.HBaseConnectionDetails;
import uk.ac.cam.cl.groupproject12.lima.web.Web;

/**
 * This is the main java class which is run for each router when we receive
 * netflow data. It will be called by the importer script.
 * 
 */
public class RunJobs {
	
	
	/** 
	 * Assume: files we get are in the form of
	 * "[routerIP]-[timestampMade]-netflow.csv"
	 * e.g. 127.0.0.1-4234243242-netflow.csv
	 *
	 */
	public static void main(String[] args) throws IOException 
	{	
		String[] tokens = args[0].split("-");
		String routerIp = tokens[0];
		long timestamp = Long.valueOf(tokens[1]);
		runJobs(routerIp, timestamp);
	}
	
	
		
	public static void runJobs(final String routerIp, final long timestamp)
	{
		
		final int jobParts = 3; // There are 3 parts:Stats,Dos1,Dos2
		// Tell the web that we have a new job
		Web.newJob(routerIp, timestamp, jobParts);

		Thread[] threads = new Thread[] { 
				new StatisticsJob().getThread(routerIp,timestamp),
				new DosJob().getThread(routerIp, timestamp),
				new SingleFlowJob().getThread(routerIp, timestamp),
				new ScanningJob().getThread(routerIp, timestamp)};
		try 
		{
			System.out.println("Starting threads...");
			for (Thread thread : threads)
			{
				thread.start();
			}		
			for (Thread thread : threads)
			{
				thread.join();
			}
			System.out.println("... All threads finished!");
		}
		catch (InterruptedException e) 
		{
			throw new RuntimeException("Unexpected interruption exception",e);
		}

		// Event monitor now runs and calls Web.updateJob(....,true) to tell the
		// web it has completely finished
		HBaseConnectionDetails hbaseConf = HBaseConnectionDetails.getDefault();
		EventMonitor.doSynchronise(new IP(routerIp), hbaseConf, timestamp);
	}


}
