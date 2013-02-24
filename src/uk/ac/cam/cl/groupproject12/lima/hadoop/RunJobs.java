package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
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

        //create the statistics job with no dependencies
        Job statisticsJob = new Job(StatisticsJob.getConf("input/netflow_anonymous.csv", "out/Statistics.bin1"));
        ControlledJob cStatisticsJob = new ControlledJob(statisticsJob,null);
        jbcntrl.addJob(cStatisticsJob); //add the statistics job to the control object
        
        //create the DOS job with a chain of dependencies within itself
        Job dosJob1 = new Job(DosJob.getConf("input/netflow_anonymous.csv", "out/Dos.bin1", 1));
        Job dosJob2 = new Job(DosJob.getConf("out/Dos.bin1", "out/Dos.bin2", 2));
        
        ControlledJob cDosJob1 = new ControlledJob(dosJob1,null);
        ControlledJob cDosJob2 = new ControlledJob(dosJob2,null);
        cDosJob2.addDependingJob(cDosJob1); //set up the dependency job1 -> job2
        jbcntrl.addJob(cDosJob1); //add the jobs to the control object
        jbcntrl.addJob(cDosJob2);
        
        //now run everything - this starts a thread which handles everything until complete (I think)
        jbcntrl.run();
    }
}



