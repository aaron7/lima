package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;


/**
 * 
 * @author ernest
 *	
 *	A class which provides several serialization related helper methods	
 *
 */
public class SerializationUtils 
{
	/**
	 * 	Converts a list of writables into a writable byte array.
	 */
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

	/**
	 * 	Converts any writable into the bytes that would be used to represent it during serialization
	 */
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
	
	/**
	 * 	Creates a DataInput object containing the given byte array.
	 */
	public static DataInput asDataInput(byte[] bytes) 
	{
		DataInputBuffer in = new DataInputBuffer();
		in.reset(bytes, bytes.length);
		return in;
	}
}
