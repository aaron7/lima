package uk.ac.cam.cl.groupproject12.lima.hbase;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import uk.ac.cam.cl.groupproject12.lima.hadoop.AutoWritable;
import uk.ac.cam.cl.groupproject12.lima.hadoop.FlowRecord;
import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;


/**
 * @author Team Lima
 *
 *	A class to represent a row in the Hbase Statistic table, 
 *	characterizing the traffic through a router in a timeframe.
 *
 *	Note: the totalDataSize is in bytes. Other fields are unit-less. 
 *
 */
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
	
	public Statistic(IP routerId, LongWritable timeFrame,
			IntWritable flowCount, IntWritable packetCount,
			LongWritable totalDataSize, IntWritable tCPCount,
			IntWritable uDPCount, IntWritable iCMPCount) {
		super();
		this.routerId = routerId;
		this.timeFrame = timeFrame;
		this.flowCount = flowCount;
		this.packetCount = packetCount;
		this.totalDataSize = totalDataSize;
		TCPCount = tCPCount;
		UDPCount = uDPCount;
		ICMPCount = iCMPCount;
	}

	/**
	 * 	Update this statistic with the information in a record. The FlowRecord must 
	 *  have the same router IP and have its start time fall in the same timeframe.
	 */
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
			//do nothing!
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((ICMPCount == null) ? 0 : ICMPCount.hashCode());
		result = prime * result
				+ ((TCPCount == null) ? 0 : TCPCount.hashCode());
		result = prime * result
				+ ((UDPCount == null) ? 0 : UDPCount.hashCode());
		result = prime * result
				+ ((flowCount == null) ? 0 : flowCount.hashCode());
		result = prime * result
				+ ((packetCount == null) ? 0 : packetCount.hashCode());
		result = prime * result
				+ ((routerId == null) ? 0 : routerId.hashCode());
		result = prime * result
				+ ((timeFrame == null) ? 0 : timeFrame.hashCode());
		result = prime * result
				+ ((totalDataSize == null) ? 0 : totalDataSize.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Statistic other = (Statistic) obj;
		if (ICMPCount == null) {
			if (other.ICMPCount != null)
				return false;
		} else if (!ICMPCount.equals(other.ICMPCount))
			return false;
		if (TCPCount == null) {
			if (other.TCPCount != null)
				return false;
		} else if (!TCPCount.equals(other.TCPCount))
			return false;
		if (UDPCount == null) {
			if (other.UDPCount != null)
				return false;
		} else if (!UDPCount.equals(other.UDPCount))
			return false;
		if (flowCount == null) {
			if (other.flowCount != null)
				return false;
		} else if (!flowCount.equals(other.flowCount))
			return false;
		if (packetCount == null) {
			if (other.packetCount != null)
				return false;
		} else if (!packetCount.equals(other.packetCount))
			return false;
		if (routerId == null) {
			if (other.routerId != null)
				return false;
		} else if (!routerId.equals(other.routerId))
			return false;
		if (timeFrame == null) {
			if (other.timeFrame != null)
				return false;
		} else if (!timeFrame.equals(other.timeFrame))
			return false;
		if (totalDataSize == null) {
			if (other.totalDataSize != null)
				return false;
		} else if (!totalDataSize.equals(other.totalDataSize))
			return false;
		return true;
	}
		
}
