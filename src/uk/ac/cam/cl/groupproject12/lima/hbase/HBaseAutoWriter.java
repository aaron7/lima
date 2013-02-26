package uk.ac.cam.cl.groupproject12.lima.hbase;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Joiner;

import uk.ac.cam.cl.groupproject12.lima.hadoop.AutoWritable;
import uk.ac.cam.cl.groupproject12.lima.hadoop.SerializationUtils;

/**
 *	Facilitates automatic transfer between any Autowritale instance and HBase provided that:
 *		
 *		1) the unqualified class name of the AutoWritable is the same as the name of a table in hbase
 *		2) for every field in the AutoWritable descendant type there is a corresponding hbase 
 *			column with name "f1:[col name]"
 */
public abstract class HBaseAutoWriter
{
	private static final HBaseConnection connection = new HBaseConnection();
	private static final byte[] FAMILY = "f1".getBytes();
	
	
	public static byte[] getTableName(Class<? extends AutoWritable> type)
	{
		String[] tokens = type.toString().split("\\.");
		return tokens[tokens.length - 1].getBytes();
	}
	
	public static byte[] getTableName(AutoWritable w)
	{
		return getTableName(w.getClass());
	}
	
	/**
	 * Grabs the byte array key for any AutoWritable. Generates bytes from fields annotated with HBaseKey, 
	 * 	representing them in their declared order as strings joined by a delimiter character. 
	 */
	public static byte[] getKey(AutoWritable w)
	{
		try {			
			List<String> keys = new ArrayList<String>();
			for (Field field : w.getAllInstanceFields())
			{
				if (field.isAnnotationPresent(HBaseKey.class))
				{
					field.setAccessible(true);
					String val = field.get(w).toString();
					keys.add(val);
				}
			}
			if (keys.isEmpty())
			{
				throw new IllegalArgumentException("Must have at least one field annotated with @HbaseKey");
			}
			String key = Joiner.on(Constants.HBASE_KEY_SEPARATOR).join(keys);
			return key.getBytes();
			
		}
		catch (IllegalAccessException e) 
		{
			//we just set it as accessible so this shouldnt happen
			throw new RuntimeException(e);
		}
	}
	
	/**
	 *  Inserts/updates a row in the hbase table corresponding with the given AutoWritable. Each field is put in its own column.
	 *  
	 *  Requires that all the fields are non-null writables
	 */
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
	
	/**
	 * Performs a get operation using the provided key and returns the result in a new instance of the given type.
	 */
	public static <T extends AutoWritable> T get(Class<T> type, byte[] key) throws IOException
	{
		try {
			byte[] tableName = getTableName(type);
			T w = (T)type.newInstance();
			getIntoObject(tableName, key, w);
			return w;
		}
		catch (IllegalAccessException e) 
		{
			throw new RuntimeException("The given class must have an accessible nullary constructor",e);
		}
		catch (InstantiationException e) 
		{
			throw new RuntimeException("The given class must have an accessible nullary constructor",e);
		} 
	}
	
	/**
	 * Gets the row in hbase with the key getKey(w) and sets all the fields in w based on the result of the get.
	 * 
	 * Requires that all fields in w are non-null Writables.
	 */
	public static void get(AutoWritable w) throws IOException
	{
		byte[] key = getKey(w);
		byte[] tableName = getTableName(w);
		getIntoObject(tableName, key, w);
	}
	
	
	private static void getIntoObject(byte[] tableName, byte[] key, AutoWritable w) throws IOException
	{
		try 
		{
			HTable table = new HTable(connection.getConfig(),tableName);
			Get get = new Get(key);
			
			for (Field field : w.getAllInstanceFields())
			{
				String columnName = field.getName();
				get.addColumn(FAMILY, columnName.getBytes());
			}
			Result result = table.get(get);
			table.close();
			if (result.isEmpty())
			{
				throw new IllegalArgumentException("No records match the given key");
			}
			
			for (Field field : w.getAllInstanceFields())
			{
				byte[] columnName = field.getName().getBytes();
				if (result.containsColumn(FAMILY, columnName))
				{
					byte[] bytes = result.getValue(FAMILY, columnName);
					Class<?> fieldClass = field.getType();
					Writable value = (Writable)fieldClass.newInstance();
					value.readFields(SerializationUtils.asDataInput(bytes));
					field.setAccessible(true);
					field.set(w, value);
				}
				else
				{
					throw new IllegalArgumentException("The given AutoWritable has fields which are not in the HBase table");
				}
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
