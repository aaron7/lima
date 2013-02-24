package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * This is the main java class which is run for each router when we receive netflow data.
 * It will be called by the importer script.
 *
 */
public class RunJobs {
    public void main(String[] args) throws IOException{
        //create a job control object
        JobControl jbcntrl = new JobControl("batchOfJobsGroup1");
        
        //create all of our jobs - I am not sure what jobs are going to be run for each router
        //at the moment I have just added in test file paths - these paths would be given in args
        Job statisticsJob = new Job(StatisticsJob.getConf("input/netflow_anonymous.csv", "out/Statistics.bin1"));
        Job dosJob = new Job(DosJob.getConf("input/netflow_anonymous.csv", "out/Dos.bin1"));
        
        //create a controlled statistics job which depend on nothing (null)
        ControlledJob cStatisticsJob = new ControlledJob(statisticsJob,null);
        
        //create a controlled dosJob that depends on statistics as an example (using an ArrayList)
        ArrayList<ControlledJob> cDosJobDependencies = new ArrayList<ControlledJob>();
        cDosJobDependencies.add(cStatisticsJob);
        ControlledJob cDosJob = new ControlledJob(dosJob,cDosJobDependencies);
        
        //add all of our jobs to the control object
        jbcntrl.addJob(cStatisticsJob);
        jbcntrl.addJob(cDosJob);

        //now run everything - this starts a thread which handles everything until complete (I think)
        jbcntrl.run();
    }
}



