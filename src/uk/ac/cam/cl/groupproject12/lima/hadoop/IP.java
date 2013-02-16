package uk.ac.cam.cl.groupproject12.lima.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class IP implements Writable{

	public String value;
	
	private IP() 
	{
		//private constructor for deserializing
	}
	
	public IP(String value) 
	{
		this.value = value;
	}
	
	public boolean isValid()
	{
		
		String[] tokens = this.value.split(".");
		if (tokens.length != 4)
		{
			return false;
		}
		for(int i =0;i<tokens.length;i++)
		{
			try {
				int a = Integer.valueOf(tokens[i]);
				
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
	
	
	@Override
	public void readFields(DataInput input) throws IOException 
	{
		this.value = Text.readString(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		Text.writeString(output, this.value);
	}

	
	
	
	
	
}
