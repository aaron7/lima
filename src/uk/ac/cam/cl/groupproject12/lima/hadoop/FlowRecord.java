package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;



/**
 * 
 * @author ernest
 *
 *	A class which acts as a container for information about a flow.
 *
 *	
 */
public class FlowRecord implements Writable{
	
	public long routerId;
	public long startTime;  	//in ms
	public long endTime;		//in ms
	public String protocol;
	public IP srcAddress;
	public IP destAddress;
	public int srcPort;
	public int destPort;
	public int packets;
	public int bytes;
	public String tcpFlags;
	public String typeOfService;
	
	
	
	public FlowRecord(long routerId, long startTime, long endTime, String protocol,
			IP srcAddress, IP destAddress, int srcPort, int destPort,
			int packets, int bytes, String tcpFlags, String typeOfService) {
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

	public static FlowRecord valueOf(String str)
	{
		String[] tokens = str.split(",");
		return new FlowRecord(
				Long.valueOf(tokens[0]),
				Long.valueOf(tokens[1]),
				Long.valueOf(tokens[2]),
				tokens[3],
				IP.valueOf(tokens[4]),
				IP.valueOf(tokens[5]),
				Integer.valueOf(tokens[6]),
				Integer.valueOf(tokens[7]),
				Integer.valueOf(tokens[8]),
				Integer.valueOf(tokens[9]),
				tokens[10],
				tokens[11]);
	}

	@Override
	public void readFields(DataInput input) throws IOException 
	{
		this.routerId = input.readLong();
		this.startTime = input.readLong();
		this.endTime = input.readLong();
		this.protocol = Text.readString(input);
		this.srcAddress.readFields(input);
		this.destAddress.readFields(input);
		this.srcPort = input.readInt();
		this.destPort = input.readInt();
		this.packets = input.readInt();
		this.bytes = input.readInt();
		this.tcpFlags = Text.readString(input);
		this.typeOfService = Text.readString(input);
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(routerId);
		output.writeLong(startTime);  	
		output.writeLong(endTime);				
		Text.writeString(output, protocol);
		srcAddress.write(output);
		destAddress.write(output);		
		output.writeInt(srcPort);
		output.writeInt(destPort);
		output.writeInt(packets);
		output.writeInt(bytes);
		Text.writeString(output, this.tcpFlags);
		Text.writeString(output, typeOfService);
	}
}
