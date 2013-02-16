package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.text.ParseException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FlowRecordTest {

	
	@Test
	public void testValueOf() throws Exception
	{
		String flowLine = "0.0.0.0,2012-08-02 00:03:45.350,2012-08-02 00:03:45.350," +
				"TCP  ,     4.6.229.242,   83.111.58.191,    80, 49933,    1000,   1.5 M,.A....,  0";
		
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
		
		FlowRecord actual = FlowRecord.valueOf(flowLine);
		Assert.assertEquals(expected, actual);
	}
	
}