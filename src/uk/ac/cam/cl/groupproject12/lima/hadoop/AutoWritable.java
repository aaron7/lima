package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;


/**
 * Acts an a base class for any Writable. Works via reflection as long as all of its fields are Writable (not primitive) and it has a non-private
 * constructor taking zero arguments.
 * 
 * @author ernest
 *
 */
public abstract class AutoWritable implements Writable 
{
	
	public static List<Field> getAllFields(Writable object)
	{
		List<Field> list = new ArrayList<Field>();
		Class<?> clss = object.getClass();
		while (! clss.equals(Object.class))
		{
			list.addAll(Arrays.asList(clss.getDeclaredFields()));
			clss = clss.getSuperclass();
		}
		return list;
	}
	
	public final void readFields(DataInput input) throws IOException {
		
		List<Field> fields = getAllFields(this);
		for (Field field : fields)
		{
			if (Modifier.isStatic(field.getModifiers()))
			{
				continue;
			}
			
			try 
			{
				field.setAccessible(true);
				@SuppressWarnings("unchecked")
				Class<? extends Writable> fieldClss = (Class<? extends Writable>)field.getType();
				Writable value = fieldClss.newInstance();
				value.readFields(input);
				field.set(this, value);
			}
			catch (IllegalAccessException e) 
			{
				//should never happen b/c we just made it accesssible!
				throw new RuntimeException(e);
			}
			catch (InstantiationException e) 
			{
				throw new RuntimeException(e);
			}
		}
	}
	
	public final void write(DataOutput output) throws IOException {
		
		List<Field> fields = getAllFields(this);
		for (Field field : fields)
		{
			if (Modifier.isStatic(field.getModifiers()))
			{
				continue;
			}
			
			try 
			{
				field.setAccessible(true);
				Writable val = (Writable)field.get(this);
				val.write(output);
			}
			catch (IllegalAccessException e) 
			{
				//should never happen b/c we just made it accesssible!
				throw new RuntimeException(e);
			}
		}
	}

	public byte[] getByteValue()
	{
		try {
			DataOutputBuffer out = new DataOutputBuffer();
			this.write(out);
			return out.getData();
		} catch (IOException e) 
		{
			throw new RuntimeException("Unexpected IO Exception",e);
		}
	}
	
	@Override
	public boolean equals(Object other)
	{
		return true;
	}
	
	@Override
	public int hashCode()
	{
		return 1;
	}
	
}
