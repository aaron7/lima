package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.IOException;

/**
 * This is the main java class which is run for each router when we receive netflow data.
 * It will be called by the importer script.
 *
 */
public class RunJobs {
    public static void main(String[] args) throws IOException{
        //Set up and run the statistics thread
        Thread statisticsThread = new Thread(){
            public void run(){
                try {
                    StatisticsJob.runJob("input/netflow_anonymous.csv", "out/Statistics.out");
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
                    DosJob.runJob("input/netflow_anonymous.csv", "out/Dos.out");
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
    }
}



