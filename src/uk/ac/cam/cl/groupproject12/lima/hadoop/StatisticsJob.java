package uk.ac.cam.cl.groupproject12.lima.hadoop;
  
  import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import uk.ac.cam.cl.groupproject12.lima.hbase.Statistic;
 
 public class StatisticsJob {
 
     public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, FlowRecord> {
 
       public void map(LongWritable key, Text value, OutputCollector<LongWritable, FlowRecord> output, Reporter reporter) throws IOException {
         String line = value.toString();
         FlowRecord record = FlowRecord.valueOf(line);
         LongWritable minute = new LongWritable(record.startTime / 60000*60000);
         output.collect(minute, record);
         
       }
     }
 
     public static class Reduce extends MapReduceBase implements Reducer<LongWritable, FlowRecord, LongWritable, Statistic> {
       public void reduce(LongWritable key, Iterator<FlowRecord> values, OutputCollector<LongWritable, Statistic> output, Reporter reporter) throws IOException {
    	   
    	   Statistic stat = null;
    	   Long timeframe = key.get();
    	   
    	   for(FlowRecord record = values.next(); values.hasNext(); record = values.next())
    	   {
    		   if (stat == null)
    		   {
    			   stat = new Statistic(record.routerId, timeframe);
    		   }
    		   stat.addFlowRecord(record);
    	   }
    	   stat.putToHbase();
    	   output.collect(key, stat);
       }
     }
 
     public static void main(String[] args) throws Exception {
       JobConf conf = new JobConf(Statistics.class);
       conf.setJobName("statistics");
 
       conf.setOutputKeyClass(LongWritable.class);
       conf.setOutputValueClass(Statistic.class);
 
       conf.setMapperClass(Map.class);
       //no combiner for now, simpler that way.
       conf.setReducerClass(Reduce.class);
 
       //TODO im a little unsure about these four:
       conf.setInputFormat(TextInputFormat.class);
       conf.setOutputFormat(TextOutputFormat.class);
       FileInputFormat.setInputPaths(conf, new Path(args[0]));
       FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
       JobClient.runJob(conf);
     }
 }
 
