package uk.ac.cam.cl.groupproject12.lima.hbase;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import uk.ac.cam.cl.groupproject12.lima.hadoop.AutoWritable;
import uk.ac.cam.cl.groupproject12.lima.hadoop.FlowRecord;
import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;

public class Statistic extends AutoWritable
{
	@HBaseKey
	IP routerId;
	@HBaseKey
	LongWritable timeFrame;
	
	IntWritable flowCount = new IntWritable(0);
	IntWritable packetCount = new IntWritable(0);
	LongWritable totalDataSize = new LongWritable(0L);
	IntWritable TCPCount = new IntWritable(0);
	IntWritable UDPCount = new IntWritable(0);
	IntWritable ICMPCount = new IntWritable(0);
	
	public Statistic(IP routerId, long timeframe) 
	{
		this.routerId = routerId;
		this.timeFrame = new LongWritable(timeframe);
	}
	
	public void addFlowRecord(FlowRecord record)
	{
		this.flowCount.set(this.flowCount.get() + 1);
		this.packetCount.set(this.packetCount.get() + record.packets.get());
		this.totalDataSize.set(this.totalDataSize.get() + record.bytes.get());
		
		if (Integer.parseInt(record.protocol.toString()) == Constants.TCP)
		{
			this.TCPCount.set(this.TCPCount.get() + 1);
		}
		else if (Integer.parseInt(record.protocol.toString()) == Constants.UDP)
		{
			this.UDPCount.set(this.UDPCount.get() + 1);
		}
		else if (Integer.parseInt(record.protocol.toString()) == Constants.ICMP)
		{
			this.ICMPCount.set(this.ICMPCount.get() + 1);
		}
		else
		{
			//TODO log error?
		}
	}
}
