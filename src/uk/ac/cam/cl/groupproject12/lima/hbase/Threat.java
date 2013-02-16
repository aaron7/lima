package uk.ac.cam.cl.groupproject12.lima.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;
import uk.ac.cam.cl.groupproject12.lima.monitor.EventType;

public class Threat implements Writable
{
	private static final byte[] QUANTIFIER = "data".getBytes();
	private static final byte[] FAMILY = "family".getBytes(); 
	
	public static class Key implements Writable
	{
		
		long timeProcessed;
		long routerId;
		EventType type;
		long startTime;
		
		private Key()
		{
			super();
		}
		
		public Key(long timeProcessed, long routerId, EventType type,
				long startTime) {
			super();
			this.timeProcessed = timeProcessed;
			this.routerId = routerId;
			this.type = type;
			this.startTime = startTime;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			this.timeProcessed = in.readLong();
			this.routerId = in.readLong();
			this.type = EventType.valueOf(Text.readString(in));
			this.startTime = in.readLong();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(timeProcessed);
			out.writeLong(routerId);
			Text.writeString(out, type.toString());
			out.writeLong(startTime);
		}
		
		public byte[] getByteValue()
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
	}
	
	Key key;
	long endTime;
	IP srcIP;
	IP destIP;
	int flowCount;
	int flowDataAvg;
	int flowDataTotal;
	

	public void putToHBase()
	{
		Put put = new Put(this.getByteKey());
		put.add(FAMILY, QUANTIFIER, getByteValue());
		//HTable threats = TODO
		// threats.put(put);
	}
	
	
	private byte[] getByteKey()
	{
		return this.key.getByteValue();
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
	
	private static Key readKey(DataInput in) throws IOException
	{
		Key key = new Key();
		key.readFields(in);
		return key;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		this.key = readKey(in);
		this.endTime = in.readLong();
		this.srcIP = IP.read(in);
		this.destIP = IP.read(in);
		this.flowCount = in.readInt();
		this.flowDataAvg = in.readInt();
		this.flowDataTotal = in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		this.key.write(out);
		out.writeLong(endTime);
		this.srcIP.write(out);
		this.destIP.write(out);
		out.writeInt(flowCount);
		out.writeInt(flowDataAvg);
		out.writeInt(flowDataTotal);
	}
}

