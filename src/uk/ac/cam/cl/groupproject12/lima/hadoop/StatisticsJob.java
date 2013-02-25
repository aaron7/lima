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
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uk.ac.cam.cl.groupproject12.lima.hbase.HBaseAutoWriter;
import uk.ac.cam.cl.groupproject12.lima.hbase.Statistic;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

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
    public static List<ControlledJob> getConf(String inputPath, String outputPath) throws IOException {
        //Set up job1 to perform Map1 and Reduce1
        Job job1 = Job.getInstance(new Configuration(), "DosJobPhase1:"+inputPath);

        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(FlowRecord.class);

        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Statistic.class);

        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath));

        List<ControlledJob> res = new ArrayList<ControlledJob>();
        res.add(new ControlledJob(job1,new ArrayList<ControlledJob>()));

        return res;
    }
}

