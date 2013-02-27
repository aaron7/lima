package uk.ac.cam.cl.groupproject12.lima.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import uk.ac.cam.cl.groupproject12.lima.hbase.HBaseAutoWriter;
import uk.ac.cam.cl.groupproject12.lima.hbase.Threat;
import uk.ac.cam.cl.groupproject12.lima.monitor.EventType;
import uk.ac.cam.cl.groupproject12.lima.web.Web;

import java.io.IOException;
import java.text.ParseException;

/**
 * Class encapsulating the Mappers, Reducers, the appropriate data structure classes,
 * and an interface to run the job.
 */
public class ScanningJob extends JobBase {

    /**
     * The first map job takes in the input from csv files, converts it into PortScan and
     * collects on sourceIP,timeframe(x,startTime),destIP,destPort, where x is 10, 60, 300. Not to mix the keys,
     * if the time frame is 6, 6 is added, if it's 300, 3 is added, and if it's 10, 1 is added.
     */
    public static class Map1
            extends
            Mapper<LongWritable, Text, PortScanKey, PortScan> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            FlowRecord record;
            try {
                record = FlowRecord.valueOf(line);
                if (record.protocol.get() == 6 || record.protocol.get() == 17) { //TCP or UDP
                    //Build up for 10s time frame
                    LongWritable timeFrame = new LongWritable((record.startTime.get() / 10000) * 10000+1); //10s
                    PortScanKey outKey = new PortScanKey(record.srcAddress, timeFrame, record.destAddress, record.destPort);
                    PortScan outVal = new PortScan(record.routerId, record.srcAddress, record.startTime,record.endTime, new IntWritable(1), new IntWritable(1), record.packets, record.bytes, new IntWritable(1));
                    context.write(outKey, outVal);

                    //60s time frame
                    outKey.timeFrame = new LongWritable((outKey.timeFrame.get()/60000)*60000+6);
                    context.write(outKey,outVal);

                    //300s time frame
                    outKey.timeFrame = new LongWritable((outKey.timeFrame.get()/300000)*300000+3);
                    context.write(outKey,outVal);
                }
            } catch (ParseException e) {
                throw new RuntimeException("Parse Error", e);
            }
        }
    }

    /**
     * The first reduce job just combines the different flows with the same key
     * and aggregates the appropriate fields. It outputs an instance of
     * PortScan class, with the key sourceIP,timeframe,destIP.
     */
    public static class Reduce1 extends Reducer<PortScanKey, PortScan, BytesWritable, BytesWritable> {

        @Override
        public void reduce(PortScanKey key, Iterable<PortScan> values,
                           Context context) throws IOException, InterruptedException {

            IP routerID = null;

            long bytes = 0, minTime = 0, maxTime = 0;
            int packets = 0, flowCount = 0;

            for (PortScan val : values) {
                if (flowCount == 0) {
                    routerID = val.routerId;
                    minTime = val.startTime.get();
                    maxTime = val.endTime.get();
                }
                bytes += val.bytes.get();
                packets += val.packets.get();
                minTime = Math.min(minTime, val.startTime.get());
                maxTime = Math.max(maxTime,val.endTime.get());
                flowCount++;
            }
            if (flowCount > 0) {
                PortScan outVal = new PortScan(routerID, key.srcIP, new LongWritable(minTime), new LongWritable(maxTime), new IntWritable(1), new IntWritable(1), new IntWritable(packets), new LongWritable(bytes), new IntWritable(flowCount));
                PortScanKey outKey = new PortScanKey(key.srcIP, key.timeFrame, key.destIP, new IntWritable(0));
                context.write(SerializationUtils.asBytesWritable(outKey), SerializationUtils.asBytesWritable(outVal));
            }
        }
    }


    /**
     * The Map job used further down the pipeline, which is an identity map which converts BytesWritable to
     * PortScanKey and PortScan.
     */
    public static class MapI extends Mapper<BytesWritable,BytesWritable,PortScanKey,PortScan> {
        @Override
        public void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(
                    SerializationUtils.asAutoWritable(PortScanKey.class,key),
                    SerializationUtils.asAutoWritable(PortScan.class,value));
        }
    }

    /**
     * The second reduce job counts the number of different ports used for a single source address.
     * It outputs an instance of PortScan class with the key sourceIP,timeframe.
     */
    public static class Reduce2 extends Reducer<PortScanKey, PortScan, BytesWritable, BytesWritable> {
        @Override
        public void reduce(PortScanKey key, Iterable<PortScan> values,Context context) throws IOException, InterruptedException {
            IP routerID = null;

            long bytes = 0, minTime = 0, maxTime = 0;
            int packets = 0, flowCount = 0, portCount = 0;

            for (PortScan val : values) {
                if (portCount == 0) {
                    routerID = val.routerId;
                    minTime = val.startTime.get();
                    maxTime = val.endTime.get();
                }
                bytes += val.bytes.get();
                packets += val.packets.get();
                minTime = Math.min(minTime, val.startTime.get());
                maxTime = Math.max(maxTime, val.endTime.get());
                flowCount +=val.flowCount.get();
                portCount++;
            }
            if (portCount > 0) {
                PortScan outVal = new PortScan(routerID, key.srcIP, new LongWritable(minTime), new LongWritable(maxTime), new IntWritable(1), new IntWritable(portCount), new IntWritable(packets), new LongWritable(bytes), new IntWritable(flowCount));
                PortScanKey outKey = new PortScanKey(key.srcIP, key.timeFrame, new IP("0.0.0.0"), new IntWritable(0));
                context.write(SerializationUtils.asBytesWritable(outKey),SerializationUtils.asBytesWritable(outVal));
            }
        }
    }

    /**
     * The third reduce job counts the number of different destination IP addresses for a single source address.
     * It outputs an instance of PortScan class with the key sourceIP,timeFrame.
     */
    public static class Reduce3 extends Reducer<PortScanKey, PortScan, PortScanKey, Threat> {
        @Override
        public void reduce(PortScanKey key, Iterable<PortScan> values,Context context) throws IOException, InterruptedException {
            IP routerID = null;

            long bytes = 0, minTime = 0, maxTime = 0;
            int packets = 0, flowCount = 0, portCount = 0, destIPCount = 0;

            for (PortScan val : values) {
                if (destIPCount == 0) {
                    routerID = val.routerId;
                    minTime = val.startTime.get();
                    maxTime = val.endTime.get();
                }
                bytes += val.bytes.get();
                packets += val.packets.get();
                minTime = Math.min(minTime, val.startTime.get());
                maxTime = Math.max(maxTime,val.endTime.get());
                flowCount +=val.flowCount.get();
                portCount +=val.destPortCount.get();
                destIPCount++;
            }
            if (destIPCount > 0) {
                //These thresholds would require some tweaking and are just a wild guess.
                //For getting the thresholds correct, testing on some actual data would be required.
                //Check whether we actually recorded a PortScan/HostScan.
                if((flowCount>10) && (bytes/flowCount)<100 && (packets/flowCount)<3 && (destIPCount>10) && (flowCount/destIPCount/portCount<2)){
                    Threat threat = new Threat(
                            new LongWritable(System.currentTimeMillis()),
                            routerID,
                            EventType.ScanningAttack,
                            new LongWritable(minTime),
                            new LongWritable(maxTime),
                            key.srcIP,
                            new IP("0.0.0.0"),
                            new IntWritable(flowCount),
                            new IntWritable(packets),
                            new LongWritable(bytes)
                    );
                    PortScanKey outKey = new PortScanKey(key.srcIP, key.timeFrame, new IP("0.0.0.0"), new IntWritable(0));
                    HBaseAutoWriter.put(threat);
                    context.write(outKey, threat);
                }
            }
        }
    }

    public static class PortScan extends AutoWritable {
        public IP routerId;
        public IP srcIP;
        public LongWritable startTime; // in ms
        public LongWritable endTime; //in ms
        public IntWritable destIPCount;
        public IntWritable destPortCount;
        public IntWritable packets;
        public LongWritable bytes;
        public IntWritable flowCount;

        //For AutoWritable
        public PortScan() {
        }

        public PortScan(IP routerId, IP srcIP, LongWritable startTime, LongWritable endTime, IntWritable destIPCount, IntWritable destPortCount, IntWritable packets, LongWritable bytes, IntWritable flowCount) {
            this.routerId = routerId;
            this.srcIP = srcIP;
            this.startTime = startTime;
            this.endTime = endTime;
            this.destIPCount = destIPCount;
            this.destPortCount = destPortCount;
            this.packets = packets;
            this.bytes = bytes;
            this.flowCount = flowCount;
        }
    }

    public static class PortScanKey extends AutoWritable {
        public IP srcIP;
        public LongWritable timeFrame;
        public IP destIP;
        public IntWritable port;

        //For AutoWritable
        public PortScanKey() {
        }

        public PortScanKey(IP srcIP, LongWritable timeFrame, IP destIP, IntWritable port) {
            this.srcIP = srcIP;
            this.timeFrame = timeFrame;
            this.destIP = destIP;
            this.port = port;
        }
    }

    /**
     * Run a new DOS JobBase
     */
    @Override
    public void runJob(String routerIp, long timestamp)
            throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = "input/" + routerIp + "-" + timestamp
                + "-netflow.csv";
        String outputPath = "out/" + routerIp + "-" + timestamp + "-scan.out";

        Job nextJob;

        //Set up the first job.
        nextJob = getNewJob(
                "ScanningJobPhase1:" + inputPath,
                PortScanKey.class,
                PortScan.class,
                BytesWritable.class,
                BytesWritable.class,
                Map1.class,
                Reduce1.class,
                TextInputFormat.class,
                SequenceFileAsBinaryOutputFormat.class,
                new Path(inputPath),
                new Path(outputPath + ".phase1")
        );
        // Run it with verbose mode for debugging purposes.
        nextJob.waitForCompletion(true);
        // JobBase done - send update to web
        Web.updateJob(routerIp, timestamp, false);


        //Set up the second job.
        nextJob = getNewJob(
                "ScanningJobPhase2:" + inputPath,
                PortScanKey.class,
                PortScan.class,
                BytesWritable.class,
                BytesWritable.class,
                MapI.class,
                Reduce2.class,
                SequenceFileAsBinaryInputFormat.class,
                SequenceFileAsBinaryOutputFormat.class,
                new Path(outputPath + ".phase1"),
                new Path(outputPath + ".phase2")
        );
        // Run it with verbose mode for debugging purposes.
        nextJob.waitForCompletion(true);
        // JobBase done - send update to web
        Web.updateJob(routerIp, timestamp, false);

        //Set up the third job
        nextJob = getNewJob(
                "ScanningJobPhase3:" + inputPath,
                PortScanKey.class,
                PortScan.class,
                PortScanKey.class,
                Threat.class,
                MapI.class,
                Reduce3.class,
                SequenceFileAsBinaryInputFormat.class,
                TextOutputFormat.class,
                new Path(outputPath + ".phase2"),
                new Path(outputPath)
        );
        // Run it with verbose mode for debugging purposes.
        nextJob.waitForCompletion(true);
        // JobBase done - send update to web
        Web.updateJob(routerIp, timestamp, false);
    }
}