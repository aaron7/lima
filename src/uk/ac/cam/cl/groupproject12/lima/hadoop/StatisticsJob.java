package uk.ac.cam.cl.groupproject12.lima.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import uk.ac.cam.cl.groupproject12.lima.hbase.HBaseAutoWriter;
import uk.ac.cam.cl.groupproject12.lima.hbase.Statistic;

import java.io.IOException;
import java.text.ParseException;

public class StatisticsJob {

    public static class Map extends Mapper<LongWritable, Text, LongWritable, FlowRecord> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            FlowRecord record;
            try
            {
                record = FlowRecord.valueOf(line);
                LongWritable minute = new LongWritable(record.startTime.get() / 60000*60000);
                context.write(minute, record);
            }
            catch (ParseException e) 
            {
                throw new RuntimeException("Parse Error",e);
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, FlowRecord, LongWritable, Statistic> {
        @Override
        public void reduce(LongWritable key, Iterable<FlowRecord> values, Context context) throws IOException, InterruptedException {

            Statistic stat = null;
            Long timeframe = key.get();

            for(FlowRecord record:values)
            {
                if (stat == null)
                {
                    stat = new Statistic(record.routerId, timeframe);
                }
                stat.addFlowRecord(record);
            }
            HBaseAutoWriter.put(stat);
            context.write(key, stat);
        }
    }
    
    /**
     * Make a new Statistics Controlled job
     * @return List<ControlledJob> for the new job(s) - only one in this case
     */
    public static void runJob(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        //Set up job1 to perform Map and Reduce
        Job job = Job.getInstance(new Configuration(), "StatistcsJobPhase1:"+inputPath);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FlowRecord.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Statistic.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //Run job and wait for completion
        //Verbose=true for debugging purposes
        job.waitForCompletion(true);
    }
}

