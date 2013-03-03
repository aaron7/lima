package uk.ac.cam.cl.groupproject12.lima.hadoop;

import org.apache.hadoop.io.*;

import java.io.*;


/**
 * A class which provides several serialisation related helper methods
 */
public class SerializationUtils {
    /**
     * @param writables
     *         A list of writables
     *
     * @return A writable byte array.
     */
    public static BytesWritable asBytesWritable(Writable... writables) {
        try {
            DataOutputBuffer out = new DataOutputBuffer();
            for (Writable writable : writables) {
                writable.write(out);
            }
            return new BytesWritable(out.getData());
        } catch (IOException e) {
            throw new RuntimeException("Unexpected IO Exception", e);
        }
    }

    /**
     * @param type
     *         An class of a given type.
     * @param bytes
     *         Byte array of fields for the given class.
     * @param <T>
     *         The generic type of the instance.
     *
     * @return A new instance of the given AutoWritable class.
     */
    public static <T extends AutoWritable> T asAutoWritable(Class<T> type, BytesWritable bytes) {
        try {
            DataInput input = SerializationUtils.asDataInput(bytes.getBytes());
            T instance = (T) type.newInstance();
            instance.readFields(input);
            return instance;
        } catch (IllegalAccessException e) {
            throw new RuntimeException("The given class must have an accessible nullary constructor", e);
        } catch (InstantiationException e) {
            throw new RuntimeException("The given class must have an accessible nullary constructor", e);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected IOException", e);
        }
    }

    /**
     * @param writable
     *         An input writable.
     *
     * @return The byte array that would represent the input after serialisation.
     */
    public static byte[] asBytes(Writable writable) {
        try {
            DataOutputBuffer out = new DataOutputBuffer();
            writable.write(out);
            return out.getData();
        } catch (IOException e) {
            throw new RuntimeException("Unexpected IO Exception", e);
        }
    }

    /**
     * @param bytes
     *         A byte array of fields.
     *
     * @return The corresponding DataInput object.
     */
    public static DataInput asDataInput(byte[] bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes, bytes.length);
        return in;
    }
}
