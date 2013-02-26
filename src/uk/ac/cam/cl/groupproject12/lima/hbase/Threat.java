package uk.ac.cam.cl.groupproject12.lima.hbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import uk.ac.cam.cl.groupproject12.lima.hadoop.AutoWritable;
import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;
import uk.ac.cam.cl.groupproject12.lima.monitor.EventType;


/**
 * 
 *	A Class used to represent a single row in the Hbase Threat table.
 *
 *	The threat was processed at timeProcessed (in unix time) in the loggind data for the router identified by routerId. 
 *	The threat consists of flowCount differnt flows, the earliest starting at startTime, the lattest ending at endTime. 
 *	flowDataTotal gives the total number of bytes carried by all of the flows considered as part of this attack.
 *
 *	If the threat targets a single IP it will be stored in destIP; if the threat originates from a single IP it will be stored in srcIP.
 */
public class Threat extends AutoWritable
{
	@HBaseKey
	LongWritable timeProcessed;
	@HBaseKey
	IP routerId;
	@HBaseKey	
	EventType type;
	@HBaseKey
	LongWritable startTime;
	
	LongWritable endTime;
	IP srcIP;
	IP destIP;
	IntWritable flowCount;
	IntWritable flowDataAvg; //TODO we should probably remove this thing, its just total/count
	LongWritable flowDataTotal;

	/**
	 * public nullary constructor for serialization. Not for other uses.
	 */
	public Threat()
	{
		 
	}

	/**
	 * Construct a Threat with only the HBaseKey fields set
	 */
	public Threat(LongWritable timeProcessed, IP routerId, EventType type,
			LongWritable startTime) {
		super();
		
		this.timeProcessed = timeProcessed;
		this.routerId = routerId;
		this.type = type;
		this.startTime = startTime;
	
		//blank non-key fields:
		this.endTime = new LongWritable();
		this.srcIP = new IP();
		this.destIP = new IP();
		this.flowCount = new IntWritable();
		this.flowDataAvg = new IntWritable();
		this.flowDataTotal = new LongWritable();
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
		return type;
	}

	public void setType(EventType type) {
		this.type = type;
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
}

