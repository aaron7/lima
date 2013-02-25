package uk.ac.cam.cl.groupproject12.lima.monitor;

public class SingleInstanceException extends Exception {
    private static final long serialVersionUID = 1L;

    SingleInstanceException(String message) {
        super(message);
    }
}
