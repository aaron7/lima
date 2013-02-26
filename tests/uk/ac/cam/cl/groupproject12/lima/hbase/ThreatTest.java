package uk.ac.cam.cl.groupproject12.lima.hbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.testng.Assert;
import org.testng.annotations.Test;
import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;
import uk.ac.cam.cl.groupproject12.lima.monitor.EventType;

import java.io.IOException;

public class ThreatTest {

	LongWritable sampleTimeProcessed = new LongWritable(System.currentTimeMillis());
	IP sampleRouterId = IP.valueOf("127.0.0.1");
	EventType sampleType = EventType.icmpFlooding;
	LongWritable sampleStartTime = new LongWritable(System.currentTimeMillis() - 29);
	
	private Threat sampleThreat() 
	{
		return new Threat(sampleTimeProcessed, sampleRouterId, sampleType, sampleStartTime);
	}

	
	private Threat sampleThreatFull() 
	{
		Threat threat = sampleThreat();
		threat.setDestIP(IP.valueOf("123.213.123.213"));
		threat.setEndTime(new LongWritable(2361906293910L));
		threat.setFlowCount(new IntWritable(45));
		threat.setPacketCount(new IntWritable(7));
		threat.setFlowDataTotal(new LongWritable(235));
		threat.setSrcIP(IP.valueOf("111.111.111.1"));
		return threat;
	}
	
	@Test 
	public void testPutGet() throws IOException
	{
		Threat stat1 = sampleThreatFull();
		HBaseAutoWriter.put(stat1);
		
		Threat stat2 = sampleThreat();
		HBaseAutoWriter.get(stat2);
		Assert.assertEquals(stat1, stat2);
	}
	

	@Test 
	public void testPutGetII() throws IOException
	{
		Threat stat1 = sampleThreatFull();
		HBaseAutoWriter.put(stat1);
		
		byte[] key = HBaseAutoWriter.getKey(stat1);
		
		Threat stat2 = HBaseAutoWriter.get(Threat.class, key);
		Assert.assertEquals(stat1, stat2);
	}
	
}
