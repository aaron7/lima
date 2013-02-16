package uk.ac.cam.cl.groupproject12.lima.hbase;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.cl.groupproject12.lima.hadoop.FlowRecord;

public class Statistic implements Writable
{
	private static final byte[] QUANTIFIER = "data".getBytes();
	private static final byte[] FAMILY = "family".getBytes(); 
	
	//key = (routerId, timeframe)
	long routerId;
	long timeFrame;
	
	//values
	int flowCount;
	int packetCount;
	int totalDataSize;
	int TCPCount;
	int UDPCount;
	int ICMPCount;
	
	public Statistic(long routerId, Long timeframe) 
	{
		this.routerId = routerId;
		this.timeFrame = timeframe;
	}

	public void setKey(long routerId, long timeframe)
	{
		this.routerId = routerId;
		this.timeFrame = timeframe;
	}
	
	public void addFlowRecord(FlowRecord record)
	{
		this.flowCount ++;
		this.packetCount += record.packets;
		this.totalDataSize += record.bytes;
		
		if ("TCP".equals(record.protocol))
		{
			this.TCPCount ++;
		}
		else if ("UDP".equals(record.protocol))
		{
			this.UDPCount ++;
		}
		else if ("ICMP".equals(record.protocol))
		{
			this.ICMPCount ++;
		}
		else
		{
			//TODO log error?
		}
	}
	
	private static byte[] getByteKey(long routerId, long timeFrame)
	{
		ByteBuffer bb = ByteBuffer.allocate(16);
		bb.putLong(routerId);
		bb.putLong(timeFrame);
		return bb.array();
	}
	
	private byte[] getByteValue()
	{
		try {
			DataOutputBuffer out = new DataOutputBuffer();
			this.write(out);
			return out.getData();
		} catch (IOException e) 
		{
			throw new RuntimeException("Unexpected IO Exception",e);
		}
	}
	
	public void putToHbase()
	{
		Put put = new Put(getByteKey(this.routerId, this.timeFrame));
		put.add(FAMILY, QUANTIFIER, this.getByteValue());
		//HTable statistics = TODO
		// statistics.put(put);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.routerId = in.readLong();
		this.timeFrame = in.readLong();
		this.flowCount = in.readInt();
		this.packetCount = in.readInt();
		this.totalDataSize = in.readInt();
		this.TCPCount = in.readInt();
		this.UDPCount = in.readInt();
		this.ICMPCount = in.readInt();
	}
	

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(routerId);
		out.writeLong(timeFrame);
		out.writeInt(flowCount);
		out.writeInt(packetCount);
		out.writeInt(totalDataSize);
		out.writeInt(TCPCount);
		out.writeInt(UDPCount);
		out.writeInt(ICMPCount);
	}
}
