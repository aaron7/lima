package uk.ac.cam.cl.groupproject12.lima.hbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import uk.ac.cam.cl.groupproject12.lima.hadoop.AutoWritable;
import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;
import uk.ac.cam.cl.groupproject12.lima.monitor.EventType;

/**
 * 
 * A Class used to represent a single row in the Hbase Threat table.
 * 
 * The threat was processed at timeProcessed (in unix time) in the loggind data
 * for the router identified by routerId. The threat consists of flowCount
 * differnt flows, the earliest starting at startTime, the lattest ending at
 * endTime. flowDataTotal gives the total number of bytes carried by all of the
 * flows considered as part of this attack.
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
	IntWritable flowDataAvg; // TODO we should probably remove this thing, its
								// just total/count
	LongWritable flowDataTotal;

	/**
	 * public nullary constructor for serialization. Not for other uses.
	 */
	public Threat() {

	}

	/**
	 * Construct a Threat with only the HBaseKey fields set
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
		this.srcIP = new IP();
		this.destIP = new IP();
		this.flowCount = new IntWritable();
		this.flowDataAvg = new IntWritable();
		this.flowDataTotal = new LongWritable();
	}

	/**
	 * Constructor to build an instance of Threat with all possible field values
	 * passed in (rather than retrieving them using the HBaseAutoWriter later).
	 * 
	 * @param timeProcessed
	 * @param routerId
	 * @param type
	 * @param startTime
	 * @param endTime
	 * @param srcIP
	 * @param destIP
	 * @param flowCount
	 * @param flowDataAvg
	 * @param flowDataTotal
	 */
	public Threat(LongWritable timeProcessed, IP routerId, EventType type,
			LongWritable startTime, LongWritable endTime, IP srcIP, IP destIP,
			IntWritable flowCount, IntWritable flowDataAvg,
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
		this.flowDataAvg = flowDataAvg;
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

	public IntWritable getFlowDataAvg() {
		return flowDataAvg;
	}

	public void setFlowDataAvg(IntWritable flowDataAvg) {
		this.flowDataAvg = flowDataAvg;
	}

	public LongWritable getFlowDataTotal() {
		return flowDataTotal;
	}

	public void setFlowDataTotal(LongWritable flowDataTotal) {
		this.flowDataTotal = flowDataTotal;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((destIP == null) ? 0 : destIP.hashCode());
		result = prime * result + ((endTime == null) ? 0 : endTime.hashCode());
		result = prime * result
				+ ((flowCount == null) ? 0 : flowCount.hashCode());
		result = prime * result
				+ ((flowDataAvg == null) ? 0 : flowDataAvg.hashCode());
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
		if (flowDataAvg == null) {
			if (other.flowDataAvg != null)
				return false;
		} else if (!flowDataAvg.equals(other.flowDataAvg))
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
	 * Produces a string representation of an instance of Threat.
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
