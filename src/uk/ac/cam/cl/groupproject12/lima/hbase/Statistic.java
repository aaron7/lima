package uk.ac.cam.cl.groupproject12.lima.hbase;


import org.apache.hadoop.io.*;
import uk.ac.cam.cl.groupproject12.lima.hadoop.*;


/**
 * A class to represent a row in the HBase Statistic table, characterising the traffic through a router in a timeframe.
 * <br />
 * <p/>
 * Note: the totalDataSize is in bytes. Other fields are unitless.
 */
public class Statistic extends AutoWritable {
    @HBaseKey
    IP routerId;
    @HBaseKey
    LongWritable timeFrame;

    IntWritable flowCount = new IntWritable(0);

    /**
     * @return The timeframe stored in this object.
     */
    public LongWritable getTimeFrame() {
        return timeFrame;
    }

    /**
     * @return The flow count stored in this object.
     */
    public IntWritable getFlowCount() {
        return flowCount;
    }

    /**
     * @return The packet count stored in this object.
     */
    public IntWritable getPacketCount() {
        return packetCount;
    }

    /**
     * @return The total data size field stored in this object.
     */
    public LongWritable getTotalDataSize() {
        return totalDataSize;
    }

    /**
     * @param timeFrame
     *         Timeframe to set within this object.
     */
    public void setTimeFrame(LongWritable timeFrame) {
        this.timeFrame = timeFrame;
    }

    /**
     * @param flowCount
     *         Flow count to set within this object.
     */
    public void setFlowCount(IntWritable flowCount) {
        this.flowCount = flowCount;
    }

    /**
     * @param packetCount
     *         Packet count to set within this object.
     */
    public void setPacketCount(IntWritable packetCount) {
        this.packetCount = packetCount;
    }

    /**
     * @param totalDataSize
     *         Total data size to set within this object.
     */
    public void setTotalDataSize(LongWritable totalDataSize) {
        this.totalDataSize = totalDataSize;
    }

    IntWritable packetCount = new IntWritable(0);
    LongWritable totalDataSize = new LongWritable(0L);
    IntWritable TCPCount = new IntWritable(0);
    IntWritable UDPCount = new IntWritable(0);
    IntWritable ICMPCount = new IntWritable(0);

    /**
     * Constructor only used during serialisation.
     */
    public Statistic() {

    }

    /**
     * Creates a new instance with the values preset.
     *
     * @param routerId
     *         The router's IP address.
     * @param timeframe
     *         The timeframe of the statistics.
     */
    public Statistic(IP routerId, long timeframe) {
        this.routerId = routerId;
        this.timeFrame = new LongWritable(timeframe);
    }

    public Statistic(IP routerId, LongWritable timeFrame,
                     IntWritable flowCount, IntWritable packetCount,
                     LongWritable totalDataSize, IntWritable TCPCount,
                     IntWritable UDPCount, IntWritable ICMPCount) {
        super();
        this.routerId = routerId;
        this.timeFrame = timeFrame;
        this.flowCount = flowCount;
        this.packetCount = packetCount;
        this.totalDataSize = totalDataSize;
        this.TCPCount = TCPCount;
        this.UDPCount = UDPCount;
        this.ICMPCount = ICMPCount;
    }

    /**
     * Updates the statistics with information from a given FlowRecord.
     *
     * @param record
     *         A FlowRecord for the same router IP and a start time within the timeframe.
     *
     * @see FlowRecord
     */
    public void addFlowRecord(FlowRecord record) {
        this.flowCount.set(this.flowCount.get() + 1);
        this.packetCount.set(this.packetCount.get() + record.packets.get());
        this.totalDataSize.set(this.totalDataSize.get() + record.bytes.get());

        if (Integer.parseInt(record.protocol.toString()) == HBaseConstants.TCP) {
            this.TCPCount.set(this.TCPCount.get() + 1);
        } else if (Integer.parseInt(record.protocol.toString()) == HBaseConstants.UDP) {
            this.UDPCount.set(this.UDPCount.get() + 1);
        } else if (Integer.parseInt(record.protocol.toString()) == HBaseConstants.ICMP) {
            this.ICMPCount.set(this.ICMPCount.get() + 1);
        } else {
            //do nothing!
        }
    }

    /**
     * Custom implementation of hashCode.
     *
     * @return An integer representing the object's hashCode.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((ICMPCount == null) ? 0 : ICMPCount.hashCode());
        result = prime * result
                + ((TCPCount == null) ? 0 : TCPCount.hashCode());
        result = prime * result
                + ((UDPCount == null) ? 0 : UDPCount.hashCode());
        result = prime * result
                + ((flowCount == null) ? 0 : flowCount.hashCode());
        result = prime * result
                + ((packetCount == null) ? 0 : packetCount.hashCode());
        result = prime * result
                + ((routerId == null) ? 0 : routerId.hashCode());
        result = prime * result
                + ((timeFrame == null) ? 0 : timeFrame.hashCode());
        result = prime * result
                + ((totalDataSize == null) ? 0 : totalDataSize.hashCode());
        return result;
    }

    /**
     * Overridden definition of equals.
     *
     * @param obj
     *         The object to compare against.
     *
     * @return Boolean true if the two objects contain the same values in all fields or are the same object entirely.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Statistic other = (Statistic) obj;
        if (ICMPCount == null) {
            if (other.ICMPCount != null)
                return false;
        } else if (!ICMPCount.equals(other.ICMPCount))
            return false;
        if (TCPCount == null) {
            if (other.TCPCount != null)
                return false;
        } else if (!TCPCount.equals(other.TCPCount))
            return false;
        if (UDPCount == null) {
            if (other.UDPCount != null)
                return false;
        } else if (!UDPCount.equals(other.UDPCount))
            return false;
        if (flowCount == null) {
            if (other.flowCount != null)
                return false;
        } else if (!flowCount.equals(other.flowCount))
            return false;
        if (packetCount == null) {
            if (other.packetCount != null)
                return false;
        } else if (!packetCount.equals(other.packetCount))
            return false;
        if (routerId == null) {
            if (other.routerId != null)
                return false;
        } else if (!routerId.equals(other.routerId))
            return false;
        if (timeFrame == null) {
            if (other.timeFrame != null)
                return false;
        } else if (!timeFrame.equals(other.timeFrame))
            return false;
        if (totalDataSize == null) {
            if (other.totalDataSize != null)
                return false;
        } else if (!totalDataSize.equals(other.totalDataSize))
            return false;
        return true;
    }

    /**
     * @return A string representing the given object.
     */
    @Override
    public String toString() {
        return "Statistic [routerId=" + routerId + ", timeFrame=" + timeFrame
                + ", flowCount=" + flowCount + ", packetCount=" + packetCount
                + ", totalDataSize=" + totalDataSize + ", TCPCount=" + TCPCount
                + ", UDPCount=" + UDPCount + ", ICMPCount=" + ICMPCount + "]";
    }
}
