package uk.ac.cam.cl.groupproject12.lima.monitor;

/**
 * A location for constant values internal to the Monitor to be specified.
 *
 * @author Team Lima
 */
public class Constants {
    public static final String DAEMON_ERROR_LOCKFILE_NON_FILE = "The lockfile at location %s is not a valid file but rather has some other file system object type.";
    public static final String DAEMON_ERROR_LOCKFILE_UNABLE_TO_ACQUIRE_LOCK = "Unable to acquire a lock on the lockfile. Another instance of this daemon may be running, thereby preventing another from starting.";
    public static final String DAEMON_LOCKFILE_NAME_DEFAULT = ".monitorLockfile";
}
