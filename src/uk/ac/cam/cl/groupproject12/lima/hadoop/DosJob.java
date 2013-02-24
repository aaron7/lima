package uk.ac.cam.cl.groupproject12.lima.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

/**
 * Encapsulates the Map and Reduce jobs for the DoS attack threat analysis.
 */
public class DosJob {
    /**
     * The firs map job takes text and produces a FlowRecord if the particular flow is suspicious.
     * The keys are based on a minute-based timestamp, destination address and source address.
     */
    public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, BytesWritable, FlowRecord> {
        //TODO determine a sensible threshold.
        public static final int bytesPacketsThreshold = 30;

        public void map(LongWritable key, Text value, OutputCollector<BytesWritable, FlowRecord> output, Reporter reporter) throws IOException {
            String line = value.toString();
            FlowRecord record;
            try
            {
                record = FlowRecord.valueOf(line);
                if(record.bytes.get()/record.packets.get()<bytesPacketsThreshold){
                    LongWritable minute = new LongWritable(record.startTime.get() / 60000*60000);
                    output.collect(SerializationUtils.asBytes(record.destAddress,minute,record.srcAddress), record);
                }
            }
            catch (ParseException e)
            {
                throw new RuntimeException("Parse Error",e);
            }
        }
    }

    /**
     * The first reduce jobs just combines the different flows with the same key and aggregates the appropriate fields.
     * It outputs an instance of DoSAttack class, with the same key.
     */
    public static class Reduce1 extends MapReduceBase implements Reducer<BytesWritable, FlowRecord, BytesWritable, DoSAttack> {
        public void reduce(BytesWritable key, Iterator<FlowRecord> values, OutputCollector<BytesWritable, DoSAttack> output, Reporter reporter) throws IOException {

            boolean first = true;
            IP routerID = null,destAddr = null;

            long startTime = 0, endTime = 0, bytes = 0;
            int packets = 0, flowCount = 0;

            for(FlowRecord record = values.next(); values.hasNext(); record = values.next())
            {
                if (first)
                {
                    routerID = record.routerId;
                    destAddr = record.destAddress;
                    startTime = record.startTime.get();
                    endTime = record.endTime.get();
                    bytes = record.bytes.get();
                    packets = record.packets.get();
                    first = false;
                } else {
                    startTime = Math.min(startTime, record.startTime.get());
                    endTime = Math.max(endTime, record.endTime.get());
                    bytes += record.bytes.get();
                    packets += record.packets.get();
                }
                flowCount++;
            }
            if(!first)
                output.collect(key,new DoSAttack(routerID,new LongWritable(startTime),new LongWritable(endTime),destAddr,new IntWritable(packets),new LongWritable(bytes),new IntWritable(flowCount),new IntWritable(1)));
        }
    }

    /**
     * The second map job only creates a key that represents the minute timestamp and the destination address of the attack.
     */
    public static class Map2 extends MapReduceBase implements Mapper<BytesWritable, DoSAttack, BytesWritable, DoSAttack> {
        public void map(BytesWritable key, DoSAttack value, OutputCollector<BytesWritable, DoSAttack> output, Reporter reporter) throws IOException {
            output.collect(SerializationUtils.asBytes(value.destAddress,new LongWritable(value.startTime.get() / 60000*60000)),value);
        }
    }

    /**
     * The second reduce job collects the data for the same key.
     * It only outputs if the whole DosAttack is determined to be significant, i.e. that the data is not noise.
     */
    public static class Reduce2 extends MapReduceBase implements Reducer<BytesWritable, DoSAttack, BytesWritable, DoSAttack> {
        public void reduce(BytesWritable key, Iterator<DoSAttack> values, OutputCollector<BytesWritable, DoSAttack> output, Reporter reporter) throws IOException {

            boolean first = true;
            IP routerID = null, destAddr = null;

            long startTime = 0, endTime = 0, bytes = 0;
            int packets = 0, flowCount = 0, srcIPCount = 0;


            for(DoSAttack dos = values.next(); values.hasNext(); dos = values.next())
            {
                if (first)
                {
                    routerID = dos.routerId;
                    destAddr = dos.destAddress;
                    startTime = dos.startTime.get();
                    endTime = dos.endTime.get();
                    bytes = dos.bytes.get();
                    packets = dos.packets.get();
                    flowCount = dos.flowCount.get();
                    first = false;
                } else {
                    startTime = Math.min(startTime, dos.startTime.get());
                    endTime = Math.max(endTime,dos.endTime.get());
                    bytes += dos.bytes.get();
                    packets += dos.packets.get();
                    flowCount += dos.flowCount.get();
                }
                srcIPCount++;
            }
            if(!first){
                DoSAttack res = new DoSAttack(routerID,new LongWritable(startTime),new LongWritable(endTime),destAddr,new IntWritable(packets),new LongWritable(bytes),new IntWritable(flowCount), new IntWritable(srcIPCount));
                if(isSignificant(res))
                    output.collect(key,res);
                //TODO output to HBase here
            }
        }

    }
    public static class DoSAttack extends AutoWritable{
        public IP routerId;
        public LongWritable startTime;  	//in ms
        public LongWritable endTime;		//in ms
        public IP destAddress;
        public IntWritable packets;
        public LongWritable bytes;
        public IntWritable flowCount;
        public IntWritable srcIPCount;

        public DoSAttack(){
            //Used by serialization
        }

        public DoSAttack(IP routerId, LongWritable startTime, LongWritable endTime, IP destAddress, IntWritable packets, LongWritable bytes, IntWritable flowCount, IntWritable srcIPCount) {
            this.routerId = routerId;
            this.startTime = startTime;
            this.endTime = endTime;
            this.destAddress = destAddress;
            this.packets = packets;
            this.bytes = bytes;
            this.flowCount = flowCount;
            this.srcIPCount = srcIPCount;
        }
    }

    /**
     * This method takes in a DoSAttack and determines whether it is significant, i.e. whether it is just noise
     * data, or whether it is an actual attack, based on the size of the flow, the packets transmitted, etc.
     * @param res
     * @return true if res is not noise data.
     */
    private static boolean isSignificant(DoSAttack res) {
        //TODO determine whether the result is significant enough to be determined as a DoS attack.
        //This is basically just guessing with our knowledge of networking, the network topology, and without the
        //ability to test on large-scale real-world data, so I suggest just guessing something and not worrying
        //about the actual numbers we put in, which would be determined by whoever actually wants to use this.
        return res.packets.get()>10000;
    }
}
