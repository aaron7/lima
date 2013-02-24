package uk.ac.cam.cl.groupproject12.lima.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;


/**
 * A test class to run the DosJob map reduce (as of now only the first stage
 * that produces binary file to be run by another stage.
 */
public class RunDosJob {
    public static void main(String[] args) throws IOException {
        //TODO find a way of linking multiple stage jobs.
        //I don't think it's necessary to use other external tools.
        //As the pipelines are static, we could instead just have describe the pipeline
        //in program. There should be a way of having jobs depending on other jobs finishing.
        //I have seen that for other version of the hadoop API, so we should find a way of
        //doing it in the new one as well.

        //I think our jar should have one class with main, which should run all the jobs that we have
        //The ones with multiple stages should be set up by that class as well.
        //That seems easiest and most fit for our purpose.

        JobConf conf = new JobConf();
        conf.setJobName("DosJob");

        //I am not sure about this line, but that seems to be what the examples are doing
        conf.setJarByClass(RunDosJob.class);

        conf.setMapOutputKeyClass(BytesWritable.class);
        conf.setMapOutputValueClass(FlowRecord.class);

        conf.setOutputKeyClass(BytesWritable.class);
        conf.setOutputKeyClass(DosJob.DoSAttack.class);

        conf.setMapperClass(DosJob.Map1.class);
        conf.setReducerClass(DosJob.Reduce1.class);

        //Should parse lines, key is byteOffset from the beginning of the file, but not used anyway
        conf.setInputFormat(TextInputFormat.class);
        //Should produce a binary format. I think this should be used for intermediate representation in
        //multi-stage MapReduce jobs. For the final output, we might want to use a text format, but as of now,
        //we are not using the files anyway.
        conf.setOutputFormat(SequenceFileAsBinaryOutputFormat.class);

        //Replace with some arguments passed, this was only for my internal testing.
        FileInputFormat.setInputPaths(conf, new Path("input/netflow_anonymous.csv"));
        FileOutputFormat.setOutputPath(conf, new Path("out/Dos.bin1"));

        //Submit this job (jobconfiguration).
        RunningJob job = JobClient.runJob(conf);

        //I want to block for completion in here. We wouldn't want to block for completion in our production code.
        job.waitForCompletion();

    }
}
