package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.DataInput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * @author Team Lima
 *
 *	A class which acts as a container for information about a flow.
 *
 *	startTime, endTime are in unix epoch format
 *	protocol is an integer with meanings in Constants.java
 *	
 */
public class FlowRecord extends AutoWritable{
	
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	public IP routerId;
	public LongWritable startTime;  	//in ms
	public LongWritable endTime;		//in ms
	public IntWritable protocol;
	public IP srcAddress;
	public IP destAddress;
	public IntWritable srcPort;
	public IntWritable destPort;
	public IntWritable packets;
	public LongWritable bytes;
	public Text tcpFlags;
	public Text typeOfService;
	
	
	
	public FlowRecord(IP routerId, long startTime, long endTime, int protocol,
			IP srcAddress, IP destAddress, int srcPort, int destPort,
			int packets, long bytes, String tcpFlags, String typeOfService) {
		super();
		this.routerId = routerId;
		this.startTime = new LongWritable(startTime);
		this.endTime =   new LongWritable(endTime);
		this.protocol = new IntWritable(protocol);
		this.srcAddress = srcAddress;
		this.destAddress = destAddress;
		this.srcPort = new IntWritable(srcPort);
		this.destPort = new IntWritable(destPort);
		this.packets = new IntWritable(packets);
		this.bytes = new LongWritable(bytes);
		this.tcpFlags = new Text(tcpFlags);
		this.typeOfService = new Text(typeOfService);
	}

	public FlowRecord() {
		super(); //private constructor for deserializing
	}

	public static long valueOfDate(String string) throws ParseException
	{
		Date date = dateFormat.parse(string);
		return date.getTime();
	}
	
	/**
	 * Interprets the value of the bytes field. Which can either be a plain interger or
	 * in the form "[integer] M" representing megabytes.
	 * 
	 */
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
	
	/**
	 * 	Factory method for generating a FlowRecord from a line of nfdump outputted with format
	 * 		"fmt:%ra,%ts,%te,%pr,%sa,%da,%sp,%dp,%pkt,%byt,%flg,%tos"
	 */
	public static FlowRecord valueOf(String str) throws ParseException
	{
		String[] tokens = str.split(" *, *");
		if (Integer.valueOf(tokens[3]) == 1) {
		    //System.out.println("test" + tokens[3] + "->" + tokens[7]);
		    //if ICMP then set the destPort to 0 since this may contain
		    //extra information which is in decimal format
		    //http://osdir.com/ml/network.netflow.nfdump.general/2007-10/msg00004.html
		    tokens[7] = "0";
		}
		return new FlowRecord(
				IP.valueOf(tokens[0]),
				valueOfDate(tokens[1]),
				valueOfDate(tokens[2]),
				Integer.valueOf(tokens[3]),
				IP.valueOf(tokens[4]),
				IP.valueOf(tokens[5]),
				Integer.valueOf(tokens[6]),
				Integer.valueOf(tokens[7]),
				Integer.valueOf(tokens[8]),
				valueOfBytes(tokens[9]),
				tokens[10],
				tokens[11]);
	}
	
	/**
	 * A factory method for reading a FlowRecord from a DataInput
	 */
	public static FlowRecord read(DataInput input) throws IOException
	{
		FlowRecord record = new FlowRecord();
		record.readFields(input);
		return record;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((bytes == null) ? 0 : bytes.hashCode());
		result = prime * result
				+ ((destAddress == null) ? 0 : destAddress.hashCode());
		result = prime * result
				+ ((destPort == null) ? 0 : destPort.hashCode());
		result = prime * result + ((endTime == null) ? 0 : endTime.hashCode());
		result = prime * result + ((packets == null) ? 0 : packets.hashCode());
		result = prime * result
				+ ((protocol == null) ? 0 : protocol.hashCode());
		result = prime * result
				+ ((routerId == null) ? 0 : routerId.hashCode());
		result = prime * result
				+ ((srcAddress == null) ? 0 : srcAddress.hashCode());
		result = prime * result + ((srcPort == null) ? 0 : srcPort.hashCode());
		result = prime * result
				+ ((startTime == null) ? 0 : startTime.hashCode());
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
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		FlowRecord other = (FlowRecord) obj;
		if (bytes == null) {
			if (other.bytes != null)
				return false;
		} else if (!bytes.equals(other.bytes))
			return false;
		if (destAddress == null) {
			if (other.destAddress != null)
				return false;
		} else if (!destAddress.equals(other.destAddress))
			return false;
		if (destPort == null) {
			if (other.destPort != null)
				return false;
		} else if (!destPort.equals(other.destPort))
			return false;
		if (endTime == null) {
			if (other.endTime != null)
				return false;
		} else if (!endTime.equals(other.endTime))
			return false;
		if (packets == null) {
			if (other.packets != null)
				return false;
		} else if (!packets.equals(other.packets))
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
		if (srcPort == null) {
			if (other.srcPort != null)
				return false;
		} else if (!srcPort.equals(other.srcPort))
			return false;
		if (startTime == null) {
			if (other.startTime != null)
				return false;
		} else if (!startTime.equals(other.startTime))
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
