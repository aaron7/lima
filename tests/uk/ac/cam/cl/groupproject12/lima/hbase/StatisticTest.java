package uk.ac.cam.cl.groupproject12.lima.hbase;

import java.io.IOException;
import java.text.ParseException;

import org.testng.annotations.Test;

import uk.ac.cam.cl.groupproject12.lima.hadoop.FlowRecord;
import uk.ac.cam.cl.groupproject12.lima.hadoop.IP;

public class StatisticTest {
	
	@Test
	public void addExampleRecord() throws ParseException, IOException{
		Statistic statistic = new Statistic(new IP("127.0.0.1"), 123456789L);
		FlowRecord testFlow = new FlowRecord(			
				IP.valueOf("0.0.0.0"), 
				FlowRecord.valueOfDate("2012-08-02 00:03:45.350"),
				FlowRecord.valueOfDate("2012-08-02 00:03:45.350"),
				Constants.TCP,
				IP.valueOf("4.6.229.242"),
				IP.valueOf("83.111.58.191"),
				80,
				49933,
				1000,
				1500000,
				".A....",
				"0");
		
		statistic.addFlowRecord(testFlow);
		statistic.putToHbase();
	}
	
}
