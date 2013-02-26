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
			throw new IllegalArgumentException(value + " is not a valid ip");
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
		String[] tokens = this.value.toString().trim().split("\\.");
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IP other = (IP) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
}
