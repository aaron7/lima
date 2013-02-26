package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import uk.ac.cam.cl.groupproject12.lima.hbase.Constants;
import uk.ac.cam.cl.groupproject12.lima.hbase.HBaseAutoWriter;
import uk.ac.cam.cl.groupproject12.lima.hbase.Threat;
import uk.ac.cam.cl.groupproject12.lima.monitor.EventType;

public class SingleFlowJob {

	private static boolean isReflectingPort(IntWritable port) {
		int portNumber = port.get();
		//TODO what are the reflecting ports?
		
		return false;
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
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, BytesWritable, SingleFlowThreat> 
	{		
		private static int largePacketCountThreshold = 475; // TODO find an appropriate value
		private static int largeBytesCountThreshold = 3700;  //TODO find an appropriate value

		public void map(LongWritable key, Text value, OutputCollector<BytesWritable, SingleFlowThreat> output, Reporter reporter) throws IOException 
		{
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
				output.collect(outputKey, threat);
			
			}catch (ParseException e) 
			{
				throw new RuntimeException("Parse Error", e);
			}
		}
	}

	
	/**
	 *	The reducer will do a single traversal of the set of flows associated with each potential attack and record the relevant stats.
	 */
	 public static class Reduce extends MapReduceBase implements Reducer<BytesWritable, SingleFlowThreat, BytesWritable, Threat> 
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
			 //TODO data avg?
			 threat.getFlowDataTotal().set(threat.getFlowDataTotal().get() + record.bytes.get());
		 }
		 
		 public void reduce(BytesWritable key, Iterator<SingleFlowThreat> values, OutputCollector<BytesWritable, Threat> output, Reporter reporter) throws IOException 
		 { 
			 
			 Threat threat = null;
			 for (SingleFlowThreat sft = values.next(); values.hasNext(); sft = values.next()) 
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
			output.collect(key, threat);
		 }
	 }
}
