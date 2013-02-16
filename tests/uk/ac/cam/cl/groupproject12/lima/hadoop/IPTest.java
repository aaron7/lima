package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IPTest {

	@Test 
	public void testValueOf()
	{
		String sample = "127.0.0.1";
		IP expected = new IP(sample);
		IP actual  = IP.valueOf(sample);
		Assert.assertEquals(actual, expected);
	}

	@Test 
	public void testIsValidPositiveCase()
	{
		String sample = "127.0.0.1";
		IP ip = new IP(sample);
		Assert.assertTrue(ip.isValid());
	}
	
	@Test 
	public void testIsValidNegativeCase()
	{
		IP ip;
		ip = new IP("127.0.0.1.3");
		Assert.assertFalse(ip.isValid());
		
		ip = new IP("127.0.01");
		Assert.assertFalse(ip.isValid());
		
		ip = new IP("127.0.01.256");
		Assert.assertFalse(ip.isValid());
		
		ip = new IP("127.0.01,1");
		Assert.assertFalse(ip.isValid());
	}
	
	
	@Test 
	public void testReadWrite() throws IOException
	{
		String sample = "127.0.0.1";
		IP expected = new IP(sample);
		DataOutputBuffer out = new DataOutputBuffer();
		expected.write(out);
		DataInputBuffer in = new DataInputBuffer();
		in.reset(out.getData(), out.getData().length);
		IP actual = IP.read(in);
		Assert.assertEquals(actual, expected);
	}
	
	
	
}
 