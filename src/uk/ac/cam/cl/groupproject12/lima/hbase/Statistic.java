package uk.ac.cam.cl.groupproject12.lima.hbase;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import uk.ac.cam.cl.groupproject12.lima.hadoop.AutoWritable;
import uk.ac.cam.cl.groupproject12.lima.hadoop.FlowRecord;
import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;

public class Statistic extends AutoWritable
{
	private static final byte[] QUANTIFIER = "data".getBytes();
	private static final byte[] FAMILY = "family".getBytes(); 
	
	public static class Key extends AutoWritable
	{
		IP routerId;
		LongWritable timeFrame;
		
		public Key(IP routerId, long timeFrame) {
			super();
			this.routerId = routerId;
			this.timeFrame = new LongWritable(timeFrame);
		}
		
		public Key() {
			super(); // constructor for deserialization
		}
	}
	
	Key key;
	IntWritable flowCount;
	IntWritable packetCount;
	LongWritable totalDataSize;
	IntWritable TCPCount;
	IntWritable UDPCount;
	IntWritable ICMPCount;
	
	public Statistic(IP routerId, Long timeframe) 
	{
		this.key = new Key(routerId, timeframe);
	}
	
	public void addFlowRecord(FlowRecord record)
	{
		this.flowCount.set(this.flowCount.get() + 1);
		this.packetCount.set(this.packetCount.get() + record.packets.get());
		this.totalDataSize.set(this.totalDataSize.get() + record.bytes.get());
		
		if ("TCP".equals(record.protocol))
		{
			this.TCPCount.set(this.TCPCount.get() + 1);
		}
		else if ("UDP".equals(record.protocol))
		{
			this.UDPCount.set(this.UDPCount.get() + 1);
		}
		else if ("ICMP".equals(record.protocol))
		{
			this.ICMPCount.set(this.ICMPCount.get() + 1);
		}
		else
		{
			//TODO log error?
		}
	}
	
	public void putToHbase()
	{
		Put put = new Put(this.key.getByteValue());
		put.add(FAMILY, QUANTIFIER, this.getByteValue());
		//HTable statistics = TODO
		// statistics.put(put);
	}
}
