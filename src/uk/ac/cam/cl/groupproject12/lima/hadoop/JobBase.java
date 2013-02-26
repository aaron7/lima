package uk.ac.cam.cl.groupproject12.lima.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public abstract class JobBase {
    public Thread getThread(final String routerIp, final String timestamp) {
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

    protected abstract void runJob(String routerIP, String timestamp)
            throws IOException,ClassNotFoundException,InterruptedException;

    /**
     * Sets up a ew job appropriately.
     * @param jobName is the name of the job.
     * @param keyMedCls is the class of the mapper output key
     * @param valMedCls is the class of the mapper output value
     * @param keyOutCls is the class of the reducer output key
     * @param valOutCls is the class of the reducer output value
     * @param mapper is the mapper that the job will run.
     * @param reducer is the reducer that the job will run.
     * @param inputFormatClass is the input format class that the job will use.
     * @param outputFormatClass is the output format class that the job will use.
     * @param inputPath is the path of the input that the job will get data from.
     * @param outputPath is the path the job will output to
     * @param <KEY_IN> is the type of the input key
     * @param <VAL_IN> is the type of the input value
     * @param <KEY_MED> is the type of the key outputted by map and received by reduce
     * @param <VAL_MED> is the type of the value outputted by map and received by reduce
     * @param <KEY_OUT> is the type of the output key
     * @param <VAL_OUT> is the type of the output value
     * @return the Job.
     * @throws java.io.IOException
     */
    protected <KEY_IN,VAL_IN,KEY_MED,VAL_MED,KEY_OUT,VAL_OUT> Job getNewJob(
            String jobName,
            Class<KEY_MED> keyMedCls,
            Class<VAL_MED> valMedCls,
            Class<KEY_OUT> keyOutCls,
            Class<VAL_OUT> valOutCls,
            Class<? extends Mapper<KEY_IN,VAL_IN,KEY_MED,VAL_MED>> mapper,
            Class<? extends Reducer<KEY_MED,VAL_MED,KEY_OUT,VAL_OUT>> reducer,
            Class<? extends InputFormat<KEY_IN,VAL_IN>> inputFormatClass,
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
