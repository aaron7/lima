package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.IOException;

import uk.ac.cam.cl.groupproject12.lima.web.Web;

/**
 * This is the main java class which is run for each router when we receive netflow data.
 * It will be called by the importer script.
 *
 */
public class RunJobs {
    public static void main(String[] args) throws IOException{
        //ASSUME: files we get are in the form of routerIP-timestampMade-netflow.csv
        //e.g. 127.0.0.1-4234243242-netflow.csv
        //TODO: uncomment this:
/*      String[] details = args[0].split("-");
        final String routerIp = details[0];
        final String timestamp = details[1]; //value to just identify a unique job
*/        
        final String routerIp = "127.0.0.1";
        final String timestamp = "12345678"; //value to just identify a unique job
        final int jobParts = 3; //There are 3 parts:Stats,Dos1,Dos2
        
        //Tell the web that we have a new job
        Web.newJob(routerIp,timestamp, jobParts);
        
        //Set up and run the statistics thread
        Thread statisticsThread = new Thread(){
            public void run(){
                try {
                    StatisticsJob.runJob(routerIp, timestamp);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        statisticsThread.run();

        //Set up and run the DoS thread.
        Thread dosThread = new Thread(){
            public void run(){
                try {
                    DosJob.runJob(routerIp, timestamp);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        dosThread.run();

        System.out.println("running...");
        try {
            statisticsThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        try {
            dosThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        System.out.println("All jobs finished.");
        //Event monitor now runs and calls Web.updateJob(....,true) to tell the web it has completely finished
    }
}



