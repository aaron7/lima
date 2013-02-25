package uk.ac.cam.cl.groupproject12.lima.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * Entry point for starting an instance of the Event Monitor daemon. Configures
 * the environment accordingly.
 *
 * @author Team Lima
 */
public class EventMonitor {
    private FileLock filelock;

    /**
     * Constructor for an instance of an EventMonitor.
     */
    EventMonitor() {
        // Enforce a single instance of the Monitor daemon.
        try {
            singletonInstance();
        } catch (SingleInstanceException e) {
            System.err.println(String
                    .format(Constants.DAEMON_ERROR_LOCKFILE_UNABLE_TO_ACQUIRE_LOCK));
            System.exit(1);
        }

        // For testing multiple instances and the lockfile.
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "localhost");
        conf.setInt(Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 2182);
    }

    /**
     * Enforces the requirement that only one instance of the daemon should run at
     * any time on the system.
     *
     * @throws SingleInstanceException
     * @throws IOException
     */
    private void singletonInstance() throws SingleInstanceException {
        String location = Constants.DAEMON_LOCKFILE_NAME_DEFAULT;
        File lockfile = new File(location);

        // Attempt to open and obtain a lock on the lockfile's channel
        try {
            // Create the file, if it does not already exist.
            lockfile.createNewFile();
            FileOutputStream out = new FileOutputStream(lockfile);
            FileChannel channel = out.getChannel();
            this.filelock = channel.tryLock();

            if (this.filelock == null) {
                throw new SingleInstanceException(location);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void finalize() {
        // Release the file lock
        try {
            this.filelock.release();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Entry point for launching an instance of the EventMonitor daemon.
     *
     * @param args
     * @throws SingleInstanceException
     */
    public static void main(String[] args) throws SingleInstanceException {
        EventMonitor e = new EventMonitor();
    }
}
