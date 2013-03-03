package uk.ac.cam.cl.groupproject12.lima.hadoop;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.*;

/**
 * Base class to get and run jobs in Hadoop.
 */
public abstract class JobBase {
    /**
     * Starts a new thread for running a job upon.
     *
     * @param routerIp
     *         Router IP for the job.
     * @param timestamp
     *         Input timestamp.
     *
     * @return A thread for which runJob is now executing.
     *
     * @see #runJob(String, long)
     */
    public Thread getThread(final String routerIp, final long timestamp) {
        return new Thread() {
            public void run() {
                try {
                    runJob(routerIp, timestamp);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * Abstract class for derived jobs to implement to run.
     */
    protected abstract void runJob(String routerIP, long timestamp)
            throws IOException, ClassNotFoundException, InterruptedException;

    /**
     * Sets up a new job.
     *
     * @param jobName
     *         The name of the job.
     * @param keyMedCls
     *         The class of the Mapper output key.
     * @param valMedCls
     *         The class of the Mapper output value.
     * @param keyOutCls
     *         The class of the Reducer output key.
     * @param valOutCls
     *         The class of the Reducer output value.
     * @param mapper
     *         The mapper that the job will run.
     * @param reducer
     *         The reducer that the job will run.
     * @param inputFormatClass
     *         The input format class the job will use.
     * @param outputFormatClass
     *         The output format class the job will use.
     * @param inputPath
     *         The path to the data that the job will read from.
     * @param outputPath
     *         The path to where the job will output.
     * @param <KEY_IN>
     *         The type of the input key.
     * @param <VAL_IN>
     *         The type of the input value.
     * @param <KEY_MED>
     *         The type of the key outputted by Map, which is also received by Reduce.
     * @param <VAL_MED>
     *         The type of the value outputted by Map, which is also received by Reduce.
     * @param <KEY_OUT>
     *         The type of the output key.
     * @param <VAL_OUT>
     *         The type of the output value.
     *
     * @return The corresponding job.
     *
     * @throws IOException
     */
    protected <KEY_IN, VAL_IN, KEY_MED, VAL_MED, KEY_OUT, VAL_OUT> Job getNewJob(
            String jobName,
            Class<KEY_MED> keyMedCls,
            Class<VAL_MED> valMedCls,
            Class<KEY_OUT> keyOutCls,
            Class<VAL_OUT> valOutCls,
            Class<? extends Mapper<KEY_IN, VAL_IN, KEY_MED, VAL_MED>> mapper,
            Class<? extends Reducer<KEY_MED, VAL_MED, KEY_OUT, VAL_OUT>> reducer,
            Class<? extends InputFormat<KEY_IN, VAL_IN>> inputFormatClass,
            Class<? extends OutputFormat> outputFormatClass,
            Path inputPath,
            Path outputPath) throws IOException {

        Job job = Job.getInstance(new Configuration(), jobName);

        job.setMapOutputKeyClass(keyMedCls);
        job.setMapOutputValueClass(valMedCls);

        job.setOutputKeyClass(keyOutCls);
        job.setOutputValueClass(valOutCls);

        job.setMapperClass(mapper);
        job.setReducerClass(reducer);

        job.setInputFormatClass(inputFormatClass);
        job.setOutputFormatClass(outputFormatClass);

        job.setJarByClass(JobBase.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

}
