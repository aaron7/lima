package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;

public class SerializationUtils 
{
	public static BytesWritable asBytes(Writable... writables)
	{
		try 
		{
			DataOutputBuffer out = new DataOutputBuffer();
			for (Writable writable : writables)
			{
				writable.write(out);
			}
			return new BytesWritable(out.getData());
		}
		catch (IOException e) 
		{
			throw new RuntimeException("Unexpected IO Exception",e);
		}
	}
	
}
