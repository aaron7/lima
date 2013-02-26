package uk.ac.cam.cl.groupproject12.lima.monitor;

public enum EventStatus {

	EVENT_OPEN(1), EVENT_RESOLVED(2);

	private final int eventID;

	EventStatus(int eventID) {
		this.eventID = eventID;
	}

	public int getEventID() {
		return this.eventID;
	}

}
