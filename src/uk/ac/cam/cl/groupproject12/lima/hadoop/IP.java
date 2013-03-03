package uk.ac.cam.cl.groupproject12.lima.hadoop;

import org.apache.hadoop.io.*;

import java.io.*;

/**
 * A Writable for representing an IPv4 address. A valid instance will have a non-null value field with four integers
 * separated by 3 periods with each int in the range [0,256).
 */
public class IP extends AutoWritable {

    /**
     * Text representation of the IP address.
     */
    public Text value;

    /**
     * Constructor for deserialisation only.
     */
    public IP() {
    }

    /**
     * Constructs and validates a new IP object.
     *
     * @param value
     *         String representing an IP.
     */
    public IP(String value) {
        this.value = new Text(value.trim());
        if (!this.isValid()) {
            throw new IllegalArgumentException(value + " is not a valid ip");
        }
    }

    /**
     * @return the IP object as a string of the form "IP(x.x.x.x)".
     */
    @Override
    public String toString() {
        return "IP(" + this.value.toString() + ")";
    }

    /**
     * @return Whether this is a valid IP object.
     */
    public boolean isValid() {
        if (this.value == null) {
            return false;
        }
        String[] tokens = this.value.toString().trim().split("\\.");
        if (tokens.length != 4) {
            return false;
        }
        for (String token : tokens) {
            try {
                int a = Integer.valueOf(token);

                if (a < 0 || a > 255) {
                    return false;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    /**
     * Factory method for constructing a new IP object.
     *
     * @param string
     *         IP address as a string.
     *
     * @return IP object representing that string.
     */
    public static IP valueOf(String string) {
        return new IP(string);
    }

    /**
     * Factory method for creating an IP object from a byte stream.
     *
     * @param input
     *         A byte stream.
     *
     * @return An IP object.
     *
     * @throws IOException
     */
    public static IP read(DataInput input) throws IOException {
        IP ip = new IP();
        ip.readFields(input);
        return ip;
    }

    /**
     * @return The value of the field 'value' within this object.
     */
    public Text getValue() {
        return value;
    }

    /**
     * Override the default hashCode implementation.
     *
     * @return A custom version of hashCode.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    /**
     * Check if two IP objects are the same.
     *
     * @param obj
     *         The object to compare against.
     *
     * @return Boolean true if the objects hold the same value, or are the same object.
     */
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
