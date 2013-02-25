package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * This is the main java class which is run for each router when we receive netflow data.
 * It will be called by the importer script.
 *
 */
public class RunJobs {
    public static void main(String[] args) throws IOException{
        //create a job control object
        JobControl jbcntrl = new JobControl("batchOfJobsGroup1");

        //create a list of jobs and add all of the jobs we want to the list
        ArrayList<ControlledJob> jobs = new ArrayList<ControlledJob>();
        jobs.addAll(StatisticsJob.getConf("input/netflow_anonymous.csv", "out/Statistics.bin1"));
       // jobs.addAll(DosJob.getConf("input/netflow_anonymous.csv", "out/Dos.bin1"));
        
        //add all the jobs to the job control
        for (ControlledJob job: jobs){
            jbcntrl.addJob(job);
        }
        
        //now run everything - this starts a thread which handles everything until complete (I think)
        jbcntrl.run();
        
        System.out.println("running...");
        
    }
}



