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
 *	Facilitates automatic transfer between any AutoWritable instance and HBase provided that:</br>
 *		
 *  1) the unqualified class name of the AutoWritable is the same as the name of a table in HBase</br>
 *  2) for every field in the AutoWritable descendant type there is a corresponding HBase
 *			column with name "f1:[col name]"
 *  @see AutoWritable
 */
public abstract class HBaseAutoWriter
{
	private static final HBaseConnection connection = new HBaseConnection();
	private static final byte[] FAMILY = "f1".getBytes();

    /**
     * Returns the table name corresponding to a class that extends AutoWritable.
     * @param type A class that extends AutoWritable.
     * @return Corresponding table name as a byte array.
     */
	public static byte[] getTableName(Class<? extends AutoWritable> type)
	{
		String[] tokens = type.toString().split("\\.");
		return tokens[tokens.length - 1].getBytes();
	}

    /**
     * Returns the table name corresponding to an AutoWritable by retrieving its class.
     * @see #getTableName(Class)
     */
	public static byte[] getTableName(AutoWritable w)
	{
		return getTableName(w.getClass());
	}
	
	/**
	 * Grabs the byte array key for any AutoWritable. Generates bytes from fields annotated with HBaseKey, 
	 * 	representing them in their declared order as strings joined by a delimiter character.7
     *
     * 	@param w AutoWritable to analyse.
     * 	@return Corresponding byte[] key.
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
				throw new IllegalArgumentException("Must have at least one field annotated with @HBaseKey");
			}
			String key = Joiner.on(HBaseConstants.HBASE_KEY_SEPARATOR).join(keys);
			return key.getBytes();
			
		}
		catch (IllegalAccessException e) 
		{
			//we just set it as accessible so this shouldn't happen
			throw new RuntimeException(e);
		}
	}
	
	/**
	 *  Inserts/updates a row in the HBase table corresponding to the given AutoWritable.
     *  Each field is put in its own column.
	 *  
	 *  Requires that all the fields are non-null writables.
     *
     *  @param w AutoWritable containing the data to update from.
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
     * @param type Input on which getTableName() can be called.
     * @param key The byte[] value of the HBase key.
     * @return A new instance of an AutoWritable equivalent to the 'type' input field.
     * @throws IOException
     * @see #getTableName(Class)
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
	 * Gets the row in HBase with the key getKey(w) and sets all the fields in w based on the result of the get.
	 * 
	 * Requires that all fields in w are non-null writables.
     *
     * @param w AutoWritable on which getTableName() is to be called.
     * @see #getTableName(uk.ac.cam.cl.groupproject12.lima.hadoop.AutoWritable)
	 */
	public static void get(AutoWritable w) throws IOException
	{
		byte[] key = getKey(w);
		byte[] tableName = getTableName(w);
		getIntoObject(tableName, key, w);
	}

    /**
     * Method to retrieve a row of from the HBase table specified and construct it into an AutoWritable.
     * @param tableName Table name to fetch from.
     * @param key Key of the row to get.
     * @param w AutoWritable to collect the result set in.
     * @throws IOException
     */
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
			throw new RuntimeException("Writable Constructor mustn't be private");
		}
		catch (InstantiationException e) 
		{
			throw new RuntimeException("Writable must have nullary constructor",e);
		}
	}
}
