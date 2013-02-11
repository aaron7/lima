package uk.ac.cam.lima.playground.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class HDFSHelloWorld {

    public static final String theFilename = "hello.txt";
    public static final String message = "Hello, world!\n";

    /**
     * Creates a file named hello.txt, writes a short message into it,
     * reads it back and prints it to the screen
     * Run in $HADOOP_HOME/bin/hadoop
     * @param args is required for a java main class
     * @throws IOException raised when conf error
     */
    public static void main (String [] args) throws IOException {

        // Configuration object uses the default parameters
        // get the handle for the file system from conf
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filenamePath = new Path(theFilename);

        try {
            if (fs.exists(filenamePath)) {
                // remove the file first if it already exists
                // false set so no recursive delete
                fs.delete(filenamePath,false); 
            }

            // write the file using the FSDataOutputStream
            // this extends java.io.DataOutputStream
            FSDataOutputStream out = fs.create(filenamePath);
            out.writeUTF(message);
            out.close();

            // read in file using FSDataInputStream and print to screen
            FSDataInputStream in = fs.open(filenamePath);
            String messageIn = in.readUTF();
            System.out.print(messageIn);
            in.close();
        } catch (IOException ioe) {
            System.err.println("IOException during operation: " + ioe.toString());
            System.exit(1);
        }
    }
}