package uk.ac.cam.cl.groupproject12.lima.monitor;

/**
 * Enumeration of all possible event types that can be identified.
 */
public enum EventType {
    /**
     * Used if the attack is a Land Attack.
     */
    landAttack,
    /**
     * Used if the attack is a TCP flood.
     */
    tcpFlooding,
    /**
     * Used if the attack is a ping pong attack.
     */
    pingPongAttack,
    /**
     * Used if the attack is a fraggle attack.
     */
    fraggleAttack,
    /**
     * Used if the attack is a UDP flood.
     */
    udpFlooding,
    /**
     * Used if the attack is an IMCP flood.
     */
    icmpFlooding,
    /**
     * Used if the attack is a Denial of Service attack.
     */
    DoSAttack,
    /**
     * Used if the attack is a scanning attack.
     */
    scanningAttack;
    //TODO: Add other event types as jobs for them are created.
}
