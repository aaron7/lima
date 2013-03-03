package uk.ac.cam.cl.groupproject12.lima.hbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import uk.ac.cam.cl.groupproject12.lima.hadoop.AutoWritable;
import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;
import uk.ac.cam.cl.groupproject12.lima.monitor.EventType;

/**
 * 
 * A Class used to represent a single row in the HBase Threat table.<br />
 * 
 * The threat was processed at timeProcessed (in UNIX time) in the logged data
 * for the router identified by routerId. The threat consists of flowCount
 * different flows, the earliest starting at startTime, the latest ending at
 * endTime. flowDataTotal gives the total number of bytes carried by all of the
 * flows considered as part of this attack.<br />
 * 
 * If the threat targets a single IP it will be stored in destIP; if the threat
 * originates from a single IP it will be stored in srcIP.
 */
public class Threat extends AutoWritable {
	@HBaseKey
	LongWritable timeProcessed;
	@HBaseKey
	IP routerId;
	@HBaseKey
	Text type;
	@HBaseKey
	LongWritable startTime;

	LongWritable endTime;
	IP srcIP;
	IP destIP;
	IntWritable flowCount;
	IntWritable packetCount; 
	LongWritable flowDataTotal;

	/**
	 * Public nullary constructor used only for serialisation.
	 */
	public Threat() {

	}

    /**
     * Constructs a Threat by setting only fields that represent parts of the key.
     *
     * @param timeProcessed The time the threat was processed.
     * @param routerId The IP address of the router the threat was logged upon.
     * @param type The type of the event from the EventType enumeration.
     * @param startTime The time of the first flow that was part of this threat.
     * @see IP
     * @see EventType
     */
	public Threat(LongWritable timeProcessed, IP routerId, EventType type,
			LongWritable startTime) {
		super();

		this.timeProcessed = timeProcessed;
		this.routerId = routerId;
		this.type = new Text(type.toString());
		this.startTime = startTime;

		// blank non-key fields:
		this.endTime = new LongWritable();
		this.srcIP = new IP("0.0.0.0");
		this.destIP = new IP("0.0.0.0");
		this.flowCount = new IntWritable();
		this.packetCount = new IntWritable();
		this.flowDataTotal = new LongWritable();
	}

	/**
     * Constructor that sets all values the Threat can have, allowing for null entries.
	 * 
	 * @param timeProcessed The time the threat was processed.
	 * @param routerId The IP address of the router the threat was logged upon.
	 * @param type The type of the event from the EventType enumeration.
	 * @param startTime The time of the first flow that was part of this threat.
	 * @param endTime The time of the last flow detected to be part of this threat.
	 * @param srcIP The single source IP involved in this threat (if applicable)
	 * @param destIP The single destination IP involved in this threat (if applicable)
	 * @param flowCount The number of flows relating to this threat.
	 * @param packetCount The number of packets sent in this threat.
	 * @param flowDataTotal The total amount of data to be transferred during this threat.
     * @see EventType
     * @see IP
     *
	 */
	public Threat(LongWritable timeProcessed, IP routerId, EventType type,
			LongWritable startTime, LongWritable endTime, IP srcIP, IP destIP,
			IntWritable flowCount, IntWritable packetCount,
			LongWritable flowDataTotal) {
		super();
		this.timeProcessed = timeProcessed;
		this.routerId = routerId;
		this.type = new Text(type.toString());
		this.startTime = startTime;
		this.endTime = endTime;
		this.srcIP = srcIP;
		this.destIP = destIP;
		this.flowCount = flowCount;
		this.packetCount = packetCount;
		this.flowDataTotal = flowDataTotal;
	}

	public LongWritable getTimeProcessed() {
		return timeProcessed;
	}

	public void setTimeProcessed(LongWritable timeProcessed) {
		this.timeProcessed = timeProcessed;
	}

	public IP getRouterId() {
		return routerId;
	}

	public void setRouterId(IP routerId) {
		this.routerId = routerId;
	}

	public EventType getType() {
		return EventType.valueOf(this.type.toString());
	}

	public void setType(EventType type) {
		this.type.set(type.toString());
	}

	public LongWritable getStartTime() {
		return startTime;
	}

	public void setStartTime(LongWritable startTime) {
		this.startTime = startTime;
	}

	public LongWritable getEndTime() {
		return endTime;
	}

	public void setEndTime(LongWritable endTime) {
		this.endTime = endTime;
	}

	public IP getSrcIP() {
		return srcIP;
	}

	public void setSrcIP(IP srcIP) {
		this.srcIP = srcIP;
	}

	public IP getDestIP() {
		return destIP;
	}

	public void setDestIP(IP destIP) {
		this.destIP = destIP;
	}

	public IntWritable getFlowCount() {
		return flowCount;
	}

	public void setFlowCount(IntWritable flowCount) {
		this.flowCount = flowCount;
	}

	public IntWritable getPacketCount() {
		return packetCount;
	}

	public void setPacketCount(IntWritable packetCount) {
		this.packetCount = packetCount;
	}

	public LongWritable getFlowDataTotal() {
		return flowDataTotal;
	}

	public void setFlowDataTotal(LongWritable flowDataTotal) {
		this.flowDataTotal = flowDataTotal;
	}

    /**
     * Custom implementation of hashCode.
     * @return An integer hashCode for the object.
     */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((destIP == null) ? 0 : destIP.hashCode());
		result = prime * result + ((endTime == null) ? 0 : endTime.hashCode());
		result = prime * result
				+ ((flowCount == null) ? 0 : flowCount.hashCode());
		result = prime * result
				+ ((packetCount == null) ? 0 : packetCount.hashCode());
		result = prime * result
				+ ((flowDataTotal == null) ? 0 : flowDataTotal.hashCode());
		result = prime * result
				+ ((routerId == null) ? 0 : routerId.hashCode());
		result = prime * result + ((srcIP == null) ? 0 : srcIP.hashCode());
		result = prime * result
				+ ((startTime == null) ? 0 : startTime.hashCode());
		result = prime * result
				+ ((timeProcessed == null) ? 0 : timeProcessed.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

    /**
     * A custom implementation of equals.
     * @param obj The onject to compare against.
     * @return The boolean true if the two objects contain identical fields or if they are the same object.
     */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Threat other = (Threat) obj;
		if (destIP == null) {
			if (other.destIP != null)
				return false;
		} else if (!destIP.equals(other.destIP))
			return false;
		if (endTime == null) {
			if (other.endTime != null)
				return false;
		} else if (!endTime.equals(other.endTime))
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
		if (flowDataTotal == null) {
			if (other.flowDataTotal != null)
				return false;
		} else if (!flowDataTotal.equals(other.flowDataTotal))
			return false;
		if (routerId == null) {
			if (other.routerId != null)
				return false;
		} else if (!routerId.equals(other.routerId))
			return false;
		if (srcIP == null) {
			if (other.srcIP != null)
				return false;
		} else if (!srcIP.equals(other.srcIP))
			return false;
		if (startTime == null) {
			if (other.startTime != null)
				return false;
		} else if (!startTime.equals(other.startTime))
			return false;
		if (timeProcessed == null) {
			if (other.timeProcessed != null)
				return false;
		} else if (!timeProcessed.equals(other.timeProcessed))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}

    /**
     * @return A string representation of the instance of Threat.
     */
	@Override
	public String toString() {
		String threatStr;
		
		threatStr = "THREAT:\n" + 
				"Router ID: " + this.routerId + "\n" + 
				"Time Processed: " + this.timeProcessed + "\n" +
				"Threat Type: " + this.type + "\n" + 
				"Start Time: " + this.startTime + "\n" + 
				"End Time: " + this.endTime + "\n" + 
				"Source IP: " + this.srcIP + "\n" + 
				"Destination IP: " + this.destIP + "\n";
		
		return threatStr;
	}
}
