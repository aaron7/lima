package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uk.ac.cam.cl.groupproject12.lima.hbase.Constants;
import uk.ac.cam.cl.groupproject12.lima.hbase.HBaseAutoWriter;
import uk.ac.cam.cl.groupproject12.lima.hbase.Threat;
import uk.ac.cam.cl.groupproject12.lima.monitor.EventType;
import uk.ac.cam.cl.groupproject12.lima.web.Web;

public class SingleFlowJob extends JobBase {

	/**
	 * returns whether the given port number is reflecting under the UDP protocol
	 */
	private static boolean isReflectingPort(IntWritable port) {
		int portNumber = port.get();
		return portNumber == 7 || portNumber == 13 || portNumber == 17 || portNumber == 19;
	}

	/**
	 *	A temporary class for passing data from the mappers to the reducers 	
	 */
	static class SingleFlowThreat extends AutoWritable 
	{
		private LongWritable timeProcessed;
		private Text attackType;
		private FlowRecord record;

		/**
		 * Empty constructor for serialization. Not for other uses.
		 */
		public SingleFlowThreat() {

		}

		public SingleFlowThreat(LongWritable timeProcessed,
				EventType attackType, FlowRecord record) {
			super();
			this.timeProcessed = timeProcessed;
			this.attackType = new Text(attackType.toString());
			this.record = record;
		}
		
		public EventType getAttackType()
		{
			return EventType.valueOf(this.attackType.toString());
		}

		public LongWritable getTimeProcessed() {
			return timeProcessed;
		}

		public FlowRecord getRecord() {
			return record;
		}
	}

	/**
	 * 	The mapper will determine the potential attack type (if any) for each flow and then will aggregate on attackType and destination IP
	 *	
	 */
	public static class Map extends Mapper<LongWritable, Text, BytesWritable, SingleFlowThreat>
	{		
		private static int largePacketCountThreshold = 475; // TODO find an appropriate value
		private static int largeBytesCountThreshold = 3700;  //TODO find an appropriate value

        @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try 
			{
				FlowRecord record = FlowRecord.valueOf(value.toString());
				boolean largeFlow = record.packets.get() > largePacketCountThreshold
						&& record.bytes.get() > largeBytesCountThreshold;

				int protocol = record.protocol.get();
				EventType type;
				if (Constants.TCP == protocol) 
				{
					if (record.srcAddress.equals(record.destAddress)
							&& record.srcPort.equals(record.destPort)) 
					{
						// land attack!
						type = EventType.landAttack;
					}
					else if (largeFlow) {
						// TCP flooding!
						type = EventType.tcpFlooding;
					}
					else
					{
						return; //ignore this flow
					}
				} 
				else if (Constants.UDP == protocol) 
				{
					if (isReflectingPort(record.destPort)) 
					{
						if (isReflectingPort(record.srcPort)) 
						{
							// Ping Pong attack!
							type = EventType.pingPongAttack;
						}
						else 
						{
							// Fraggle attack!
							type = EventType.fraggleAttack;
						}
					}
					else if (largeFlow) 
					{
						// UDP flooding!
						type = EventType.udpFlooding;
					}
					else
					{
						return; //ignore this flow
					}
				}
				else if (Constants.ICMP == protocol) 
				{
					if (largeFlow) {
						// ICMP flooding!
						type = EventType.icmpFlooding;
					}
					else
					{
						return; //ignore this flow
					}
				}
				else 
				{
					return; // ignore this flow
				}
				SingleFlowThreat threat = new SingleFlowThreat(new LongWritable(System.currentTimeMillis()), type, record);
				BytesWritable outputKey = SerializationUtils.asBytesWritable(threat.attackType,record.destAddress);
				context.write(outputKey, threat);
			
			}catch (ParseException e) 
			{
				throw new RuntimeException("Parse Error", e);
			}
		}
	}

	
	/**
	 *	The reducer will do a single traversal of the set of flows associated with each potential attack and record the relevant stats.
	 */
	 public static class Reduce extends Reducer<BytesWritable, SingleFlowThreat, BytesWritable, Threat>
	 {
		 
		 private static void updateThreat(Threat threat, FlowRecord record)
		 {
			 if (record.startTime.get() < threat.getStartTime().get())
			 {
				 threat.setStartTime(record.startTime);
			 }
			 if (record.endTime.get() > threat.getEndTime().get())
			 {
				 threat.setEndTime(record.endTime);
			 }
			 // leave the srcIP blank
			 threat.getFlowCount().set(threat.getFlowCount().get() + 1);
			 threat.getPacketCount().set(threat.getPacketCount().get() + record.packets.get());
			 threat.getFlowDataTotal().set(threat.getFlowDataTotal().get() + record.bytes.get());
		 }

         @Override
		 public void reduce(BytesWritable key, Iterable<SingleFlowThreat> values, Context context) throws IOException, InterruptedException {
			 
			 Threat threat = null;
			 for (SingleFlowThreat sft : values)
			 {
				 FlowRecord record = sft.getRecord(); 
				 if (threat == null)
				 {	 //set field
					 threat = new Threat(sft.getTimeProcessed(), record.routerId, sft.getAttackType(), record.startTime);
					 threat.setDestIP(record.destAddress); //all destination IPs are the same
				 }
				 updateThreat(threat, record);
			 } 
			HBaseAutoWriter.put(threat);
			context.write(key, threat);
		 }
	 }

    /**
     * Run a new SingleFlowJob job
     */
    @Override
    public void runJob(String routerIp, long timestamp) throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = "input/"+routerIp+"-"+timestamp+"-netflow.csv";
        String outputPath = "out/"+routerIp+"-"+timestamp+"-singleFlow.out";

        //Set up the first job to perform Map1 and Reduce1.
        Job job = getNewJob(
                "SingleFlowJob:"+ inputPath,
                BytesWritable.class,
                SingleFlowThreat.class,
                BytesWritable.class,
                Threat.class,
                Map.class,
                Reduce.class,
                TextInputFormat.class,
                TextOutputFormat.class,
                new Path(inputPath),
                new Path(outputPath)
        );

//        protected <KEY_IN,VAL_IN,KEY_MED,VAL_MED,KEY_OUT,VAL_OUT> Job getNewJob(
//                String jobName,
//                Class<KEY_MED> keyMedCls,
//                Class<VAL_MED> valMedCls,
//                Class<KEY_OUT> keyOutCls,
//                Class<VAL_OUT> valOutCls,
//                Class<? extends Mapper<KEY_IN,VAL_IN,KEY_MED,VAL_MED>> mapper,
//                Class<? extends Reducer<KEY_MED,VAL_MED,KEY_OUT,VAL_OUT>> reducer,
//                Class<? extends InputFormat<KEY_IN,VAL_IN>> inputFormatClass,
//                Class<? extends OutputFormat<KEY_OUT,VAL_OUT>> outputFormatClass,
//                Path inputPath,
//                Path outputPath)

        //Run job and wait for completion
        //Verbose=true for debugging purposes
        job.waitForCompletion(true);

        //job done - tell web
        Web.updateJob(routerIp, timestamp, false);
    }
}
