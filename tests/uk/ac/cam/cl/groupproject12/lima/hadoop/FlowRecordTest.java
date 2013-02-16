package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.text.ParseException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FlowRecordTest {

	private static String sampleLine = "0.0.0.0,2012-08-02 00:03:45.350,2012-08-02 00:03:45.350," +
			"TCP  ,     4.6.229.242,   83.111.58.191,    80, 49933,    1000,   1.5 M,.A....,  0";
	
	private static FlowRecord getSampleRecord()
	{
		FlowRecord expected;
		try {
			expected = new FlowRecord(			
					IP.valueOf("0.0.0.0"), 
					FlowRecord.valueOfDate("2012-08-02 00:03:45.350"),
					FlowRecord.valueOfDate("2012-08-02 00:03:45.350"),
					"TCP",
					IP.valueOf("4.6.229.242"),
					IP.valueOf("83.111.58.191"),
					80,
					49933,
					1000,
					1500000,
					".A....",
					"0");
		} catch (ParseException e) 
		{
			//this shouldnt happen
			throw new RuntimeException(e);
		}
		return expected;
	}
	
	@Test
	public void testValueOf() throws Exception
	{
		FlowRecord actual = FlowRecord.valueOf(sampleLine);
		Assert.assertEquals(getSampleRecord(), actual);

	}	 
	
	@Test 
	public void testReadWrite() throws Exception
	{
		FlowRecord expected = getSampleRecord();
		DataOutputBuffer out = new DataOutputBuffer();
		expected.write(out);
		DataInputBuffer in = new DataInputBuffer();
		in.reset(out.getData(), out.getData().length);
		FlowRecord actual = FlowRecord.read(in);
		Assert.assertEquals(expected, actual);
	}
	
}