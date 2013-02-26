package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public class IP extends AutoWritable {

	public Text value;
	
	public IP() 
	{
		// constructor for deserializing
	}
	
	public IP(String value) 
	{
		this.value = new Text(value);
	}
	
	@Override
	public String toString()
	{
		return "IP(" + this.value.toString() + ")";
	}
	
	
	public boolean isValid()
	{
		
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

	public static IP valueOf(String string) 
	{
		return new IP(string);
	}

	public static IP read(DataInput input) throws IOException
	{
		IP ip = new IP();
		ip.readFields(input);
		return ip;
	}
}
