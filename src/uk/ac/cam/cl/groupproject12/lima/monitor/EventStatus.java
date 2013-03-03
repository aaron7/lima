package uk.ac.cam.cl.groupproject12.lima.monitor;

/**
 * Enumeration of all possible statuses that a given event can be in.
 */
public enum EventStatus {
    /**
     * The event is open or unhandled.
     */
	event_open,
    /**
     * The event has been closed and marked as completed.
     */
    event_resolved;
}
