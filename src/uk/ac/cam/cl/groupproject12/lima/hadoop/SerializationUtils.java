package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;

public class SerializationUtils 
{
	public static BytesWritable asBytesWritable(Writable... writables)
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

	public static byte[] asBytes(Writable[] array) 
	{
		return asBytesWritable(array).getBytes();
	}

	public static byte[] asBytes(Writable writable)
	{
		try 
		{
			DataOutputBuffer out = new DataOutputBuffer();
			writable.write(out);
			return out.getData();
		}
		catch (IOException e) 
		{
			throw new RuntimeException("Unexpected IO Exception",e);
		}
	}
	
	public static DataInput asDataInput(byte[] bytes) 
	{
		DataInputBuffer in = new DataInputBuffer();
		in.reset(bytes, bytes.length);
		return in;
	}
	
}
