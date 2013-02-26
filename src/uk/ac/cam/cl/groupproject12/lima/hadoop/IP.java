package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

/**
 * @author Team Lima
 * 
 * A Writable for representing an IPv4 address. A valid instance will have a non-null value field
 * with four integers separated by 3 periods with each int in the range [0,256). 
 * 
 */
public class IP extends AutoWritable {

	public Text value;
	
	
	/**
	 * Constructor for deserialization. Not for other use.
	 */
	public IP() 
	{
	}
	
	/**
	 *	Constructs and validates a new IP object 
	 */
	public IP(String value) 
	{
		this.value = new Text(value);
		if (! this.isValid() )
		{
			throw new IllegalArgumentException();
		}
	}
	
	@Override
	public String toString()
	{
		return "IP(" + this.value.toString() + ")";
	}
	
	/**
	 * @return whether this is a valid IP object
	 */
	public boolean isValid()
	{
		if (this.value == null)
		{
			return false;
		}
		String[] tokens = this.value.toString().split("\\.");
		if (tokens.length != 4)
		{
			return false;
		}
        for(String token : tokens)
		{
			try {
				int a = Integer.valueOf(token);

				if (a < 0 || a > 255)
				{
					return false;
				}
			} catch (NumberFormatException e)
			{
				return false;
			}
		}
		return true;
	}

	/**
	 *  Factory method for constructing a new IP object
	 */
	public static IP valueOf(String string) 
	{
		return new IP(string);
	}

	/**
	 * 	Factory method for creating IP object from a byte stream
	 */
	public static IP read(DataInput input) throws IOException
	{
		IP ip = new IP();
		ip.readFields(input);
		return ip;
	}

	/**
	 * @return the value
	 */
	public Text getValue() {
		return value;
	}
}
