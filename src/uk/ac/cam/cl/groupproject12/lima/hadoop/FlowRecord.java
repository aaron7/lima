package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author ernest
 *
 *	A class which acts as a container for information about a flow.
 */
public class FlowRecord implements Writable{
	
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	public IP routerId;
	public long startTime;  	//in ms
	public long endTime;		//in ms
	public String protocol;
	public IP srcAddress;
	public IP destAddress;
	public int srcPort;
	public int destPort;
	public int packets;
	public long bytes;
	public String tcpFlags;
	public String typeOfService;
	
	
	
	public FlowRecord(IP routerId, long startTime, long endTime, String protocol,
			IP srcAddress, IP destAddress, int srcPort, int destPort,
			int packets, long bytes, String tcpFlags, String typeOfService) {
		super();
		this.routerId = routerId;
		this.startTime = startTime;
		this.endTime = endTime;
		this.protocol = protocol;
		this.srcAddress = srcAddress;
		this.destAddress = destAddress;
		this.srcPort = srcPort;
		this.destPort = destPort;
		this.packets = packets;
		this.bytes = bytes;
		this.tcpFlags = tcpFlags;
		this.typeOfService = typeOfService;
	}

	private FlowRecord() {
		super(); //private constructor for deserializing
	}

	static long valueOfDate(String string) throws ParseException
	{
		Date date = dateFormat.parse(string);
		return date.getTime();
	}
	
	
	static long valueOfBytes(String string)
	{
		if (string.matches("\\d+(.\\d*) M"))
		{
			String[] tokens = string.split(" ");
			Double prefix = Double.valueOf(tokens[0]);
			return (long)(prefix*1000000);
		}
		return Integer.valueOf(string);
	}
	
	public static FlowRecord valueOf(String str) throws ParseException
	{
		String[] tokens = str.split(" *, *");
		return new FlowRecord(
				IP.valueOf(tokens[0]),
				valueOfDate(tokens[1]),
				valueOfDate(tokens[2]),
				tokens[3],
				IP.valueOf(tokens[4]),
				IP.valueOf(tokens[5]),
				Integer.valueOf(tokens[6]),
				Integer.valueOf(tokens[7]),
				Integer.valueOf(tokens[8]),
				valueOfBytes(tokens[9]),
				tokens[10],
				tokens[11]);
	}
	
	public static FlowRecord read(DataInput input) throws IOException
	{
		FlowRecord record = new FlowRecord();
		record.readFields(input);
		return record;
	}

	@Override
	public void readFields(DataInput input) throws IOException 
	{
		this.routerId = IP.read(input);
		this.startTime = input.readLong();
		this.endTime = input.readLong();
		this.protocol = Text.readString(input);
		this.srcAddress.readFields(input);
		this.destAddress.readFields(input);
		this.srcPort = input.readInt();
		this.destPort = input.readInt();
		this.packets = input.readInt();
		this.bytes = input.readLong();
		this.tcpFlags = Text.readString(input);
		this.typeOfService = Text.readString(input);
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
		routerId.write(output);
		output.writeLong(startTime);  	
		output.writeLong(endTime);				
		Text.writeString(output, protocol);
		srcAddress.write(output);
		destAddress.write(output);		
		output.writeInt(srcPort);
		output.writeInt(destPort);
		output.writeInt(packets);
		output.writeLong(bytes);
		Text.writeString(output, this.tcpFlags);
		Text.writeString(output, typeOfService);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (bytes ^ (bytes >>> 32));
		result = prime * result
				+ ((destAddress == null) ? 0 : destAddress.hashCode());
		result = prime * result + destPort;
		result = prime * result + (int) (endTime ^ (endTime >>> 32));
		result = prime * result + packets;
		result = prime * result
				+ ((protocol == null) ? 0 : protocol.hashCode());
		result = prime * result
				+ ((routerId == null) ? 0 : routerId.hashCode());
		result = prime * result
				+ ((srcAddress == null) ? 0 : srcAddress.hashCode());
		result = prime * result + srcPort;
		result = prime * result + (int) (startTime ^ (startTime >>> 32));
		result = prime * result
				+ ((tcpFlags == null) ? 0 : tcpFlags.hashCode());
		result = prime * result
				+ ((typeOfService == null) ? 0 : typeOfService.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FlowRecord other = (FlowRecord) obj;
		if (bytes != other.bytes)
			return false;
		if (destAddress == null) {
			if (other.destAddress != null)
				return false;
		} else if (!destAddress.equals(other.destAddress))
			return false;
		if (destPort != other.destPort)
			return false;
		if (endTime != other.endTime)
			return false;
		if (packets != other.packets)
			return false;
		if (protocol == null) {
			if (other.protocol != null)
				return false;
		} else if (!protocol.equals(other.protocol))
			return false;
		if (routerId == null) {
			if (other.routerId != null)
				return false;
		} else if (!routerId.equals(other.routerId))
			return false;
		if (srcAddress == null) {
			if (other.srcAddress != null)
				return false;
		} else if (!srcAddress.equals(other.srcAddress))
			return false;
		if (srcPort != other.srcPort)
			return false;
		if (startTime != other.startTime)
			return false;
		if (tcpFlags == null) {
			if (other.tcpFlags != null)
				return false;
		} else if (!tcpFlags.equals(other.tcpFlags))
			return false;
		if (typeOfService == null) {
			if (other.typeOfService != null)
				return false;
		} else if (!typeOfService.equals(other.typeOfService))
			return false;
		return true;
	}
}
