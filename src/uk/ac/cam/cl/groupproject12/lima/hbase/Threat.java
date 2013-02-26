package uk.ac.cam.cl.groupproject12.lima.hbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import uk.ac.cam.cl.groupproject12.lima.hadoop.AutoWritable;
import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;
import uk.ac.cam.cl.groupproject12.lima.monitor.EventType;


/**
 * 
 *	A Class used to represent a single row in the Hbase Threat table
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
	IntWritable flowDataAvg;
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

}

