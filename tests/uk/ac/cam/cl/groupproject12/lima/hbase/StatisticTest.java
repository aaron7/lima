package uk.ac.cam.cl.groupproject12.lima.hbase;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import uk.ac.cam.cl.groupproject12.lima.hadoop.FlowRecord;
import uk.ac.cam.cl.groupproject12.lima.hadoop.FlowRecordTest;
import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;

public class StatisticTest 
{
	
	private static IP sampleIP = new IP("127.0.0.1");
	private static long sampleTimeFrame = 123456789L;
	
	private static Statistic sampleStatistic()
	{
		return new Statistic(sampleIP, sampleTimeFrame);
	}
	
	private static Statistic sampleStatisticFull()
	{
		return new Statistic(sampleIP, 
				new LongWritable(sampleTimeFrame),
				new IntWritable(7), 
				new IntWritable(14), 
				new LongWritable(12),
				new IntWritable(67), 
				new IntWritable(23), 
				new IntWritable(45));
	}
	
	@Test
	public void addExampleRecord() throws ParseException, IOException{
		Statistic stat1 = sampleStatistic();
		FlowRecord testFlow = FlowRecordTest.getSampleRecord();
		stat1.addFlowRecord(testFlow);
		Statistic stat2 = sampleStatistic();
		
		stat2.flowCount.set(stat2.flowCount.get() + 1);
		stat2.packetCount.set(stat2.packetCount.get() + testFlow.packets.get());
		stat2.TCPCount.set(stat2.TCPCount.get() + 1);
		stat2.totalDataSize.set(stat2.totalDataSize.get() + testFlow.bytes.get());
		Assert.assertEquals(stat1, stat2);
	}	
	
	@Test 
	public void testPutGet() throws IOException
	{
		Statistic stat1 = sampleStatisticFull();
		HBaseAutoWriter.put(stat1);
		
		Statistic stat2 = sampleStatistic();
		HBaseAutoWriter.get(stat2);
		Assert.assertEquals(stat1, stat2);
	}
	
}
