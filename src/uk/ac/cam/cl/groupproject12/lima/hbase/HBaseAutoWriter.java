package uk.ac.cam.cl.groupproject12.lima.hbase;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.cl.groupproject12.lima.hadoop.AutoWritable;
import uk.ac.cam.cl.groupproject12.lima.hadoop.SerializationUtils;

/**
 *	Facilitates automatic transfer between any Autowritale instance and HBase provided that:
 *		
 *		1) the class name of the AutoWritable is the same as the name of a table in hbase
 *		2) for every field in the AutoWritable descendent type there is a corresponding hbase 
 *			column with name "family:[col name]"
 */
public abstract class HBaseAutoWriter
{
	private static final HBaseConnection connection = new HBaseConnection();
	private static final byte[] FAMILY = "f1".getBytes();
	
	
	private static byte[] getTableName(AutoWritable w)
	{
		String[] tokens = w.getClass().toString().split("\\.");
		return tokens[tokens.length - 1].getBytes();
	}
	
	public static byte[] getKey(AutoWritable w)
	{
		try {			
			StringBuffer sb = new StringBuffer();
			for (Field field : w.getAllInstanceFields())
			{
				if (field.isAnnotationPresent(HBaseKey.class))
				{
					field.setAccessible(true);
					String val = field.get(w).toString();
					sb.append(val);
				}
			}
			String key = sb.toString();
			if (key.isEmpty())
			{
				throw new IllegalArgumentException("Must have at least one field annotated with @HbaseKey");
			}
			return key.getBytes();
		}
		catch (IllegalAccessException e) 
		{
			//we just set it as accessible so this shouldnt happen
			throw new RuntimeException(e);
		}
	}
	
	
	public static void put(AutoWritable w) throws IOException
	{
		HTable table = new HTable(connection.getConfig(), getTableName(w));
		Put put = new Put(getKey(w));
		try 
		{
			for (Field field : w.getAllInstanceFields())
			{
				String columnName = field.getName();
				field.setAccessible(true);
				Writable value = (Writable)field.get(w);
				byte[] bytes = SerializationUtils.asBytes(value);
				put.add(FAMILY, columnName.getBytes(), bytes);
			}
		} catch (IllegalAccessException e) 
		{
			throw new RuntimeException("Unexpected exception -- we just set it to accessible");
		}
		table.put(put);
		table.close();
	}
	
	public static void get(AutoWritable w) throws IOException
	{
		HTable table = new HTable(connection.getConfig(),getTableName(w));
		Get get = new Get(getKey(w));
		try 
		{
			for (Field field : w.getAllInstanceFields())
			{
				String columnName = field.getName();
				get.addColumn(FAMILY, columnName.getBytes());
			}
			Result result = table.get(get);
			table.close();
			for (Field field : w.getAllInstanceFields())
			{
				String columnName = field.getName();
				byte[] bytes = result.getValue(FAMILY, columnName.getBytes());
				Class<?> fieldClass = field.getType();
				Writable value = (Writable)fieldClass.newInstance();
				value.readFields(SerializationUtils.asDataInput(bytes));
				field.setAccessible(true);
				field.set(w, value);
			}
		}
		catch (IllegalAccessException e) 
		{
			throw new RuntimeException("Writable Constructor musnt be private");
		}
		catch (InstantiationException e) 
		{
			throw new RuntimeException("Writable must have nullary constructor",e);
		}
	}
}
