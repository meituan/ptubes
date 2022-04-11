package com.meituan.ptubes.sdk;

import java.util.List;
import com.meituan.ptubes.sdk.model.wrapper.AckEventWrapper;
import com.meituan.ptubes.sdk.protocol.RdsPacket.RdsEvent;

/**
 *  A IRdsCdcEventListener object is used to handle the delivered rds cdc event.
 */
public interface IRdsCdcEventListener {

    /**
     * It is not recommend to throw exception, rather than returning RdsCdcEventStatus.FAILURE if
     * consumption failure
     *
     * @param events consumed batch RdsEvent list
     * @return The consume status
     */
    default RdsCdcEventStatus onEvents(final List<RdsEvent> events) {
        return RdsCdcEventStatus.FAILURE;}

    /**
     * for tidb and mysql source with ack
     *
     * @param ackEventWrapper
     * @return
     */
    @Deprecated
    default RdsCdcEventStatus onEventsWithAck(AckEventWrapper ackEventWrapper) {
        return RdsCdcEventStatus.FAILURE;
    }

    /**
     * which acknowledge type is confirmed;
     * we will cache the value, so this method should not be modified once the connector is started.
     *  (Not finished yet)
     * @return
     */
    default AckType ackType() {
        return AckType.WITHOUT_ACK;
    }

}
