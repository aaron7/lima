package uk.ac.cam.cl.groupproject12.lima.hbase;


import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import uk.ac.cam.cl.groupproject12.lima.hadoop.AutoWritable;
import uk.ac.cam.cl.groupproject12.lima.hadoop.FlowRecord;
import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;

public class Statistic extends AutoWritable
{
	private static final HBaseConnection connection = new HBaseConnection();
	
	private static final byte[] QUANTIFIER = "data".getBytes();
	private static final byte[] FAMILY = "family".getBytes(); 
	private static final byte[] STATISTICS_TABLE = "statistics".getBytes(); 
	
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

	
	IntWritable flowCount = new IntWritable(0);
	IntWritable packetCount = new IntWritable(0);
	LongWritable totalDataSize = new LongWritable(0L);
	IntWritable TCPCount = new IntWritable(0);
	IntWritable UDPCount = new IntWritable(0);
	IntWritable ICMPCount = new IntWritable(0);
	
	
	Text keyText; //
	
	public Statistic(IP routerId, Long timeframe) 
	{
		this.key = new Key(routerId, timeframe);
		keyText = new Text(routerId + "+" + timeframe); //
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
	
	public void putToHbase() throws IOException
	{
		HTable statTable = new HTable(connection.getConfig(),STATISTICS_TABLE);
		
		//Put put = new Put(this.key.getByteValue());
		//put.add(FAMILY, QUANTIFIER, this.getByteValue());
		//statTable.put(put);
		
		
		Put put = new Put(keyText.getBytes()); //routerId + timestamp
		put.add(FAMILY, "flowCount".getBytes(), getBytesInt(flowCount));

		
		LongWritable timeFrame;
		IntWritable flowCount = new IntWritable(0);
		IntWritable packetCount = new IntWritable(0);
		LongWritable totalDataSize = new LongWritable(0L);
		IntWritable TCPCount = new IntWritable(0);
		IntWritable UDPCount = new IntWritable(0);
		IntWritable ICMPCount = new IntWritable(0);
		

		statTable.close();
	}
	
	private byte[] getBytesInt(IntWritable value){
		int valueInt = value.get();
		
		return new byte[] {
	            (byte)(valueInt >>> 24),
	            (byte)(valueInt >>> 16),
	            (byte)(valueInt >>> 8),
	            (byte)valueInt};
	}
}
