package uk.ac.cam.cl.groupproject12.lima.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import uk.ac.cam.cl.groupproject12.lima.hbase.HBaseAutoWriter;
import uk.ac.cam.cl.groupproject12.lima.hbase.Statistic;
import uk.ac.cam.cl.groupproject12.lima.web.Web;

import java.io.IOException;
import java.text.ParseException;

/**
 * Mapper and Reducer used to generate statistics for a given router over a given timeframe.
 */
public class StatisticsJob extends JobBase {

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
     * Runs a new statistics job.
     * @param routerIp IP of the router the flow comes from.
     * @param timestamp Timestamp of the file the flow comes from.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Override
    public void runJob(String routerIp, long timestamp) throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = "input/"+routerIp+"-"+timestamp+"-netflow.csv";
        String outputPath = "out/"+routerIp+"-"+timestamp+"-statistics.out";

        //Set up the first job to perform Map1 and Reduce1.
        Job job = getNewJob(
                "StatisticsJob:"+ inputPath,
                LongWritable.class,
                FlowRecord.class,
                LongWritable.class,
                Statistic.class,
                Map.class,
                Reduce.class,
                TextInputFormat.class,
                TextOutputFormat.class,
                new Path(inputPath),
                new Path(outputPath)
        );

        //Run job and wait for completion
        //Verbose=true for debugging purposes
        job.waitForCompletion(true);

        //job done - tell web
        Web.updateJob(routerIp, timestamp, false);
    }
}

