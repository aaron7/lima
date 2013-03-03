package uk.ac.cam.cl.groupproject12.lima.hbase;

/**
 * A location for constant values internal to HBase to be specified.
 */
public class HBaseConstants {
    /**
     * IANA-assigned protocol number for ICMP.
     *
     * @see <a href="https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xml">IANA Protocol Numbers</a>
     */
    public static final int ICMP = 1;
    /**
     * IANA-assigned protocol number for UDP.
     *
     * @see <a href="https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xml">IANA Protocol Numbers</a>
     */
    public static final int UDP = 17;
    /**
     * IANA-assigned protocol number for TCP.
     *
     * @see <a href="https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xml">IANA Protocol Numbers</a>
     */
    public static final int TCP = 6;

    /**
     * Constant used to identify the separator symbol within HBase keys.
     */
    public static final String HBASE_KEY_SEPARATOR = "+";

    /**
     * Zookeeper Quorum parameter.
     */
    public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    /**
     * Zookeeper client port parameter.
     */
    public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
}
