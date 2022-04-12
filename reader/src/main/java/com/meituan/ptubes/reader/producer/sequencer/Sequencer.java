package com.meituan.ptubes.reader.producer.sequencer;

public interface Sequencer {

    /**
     * reset Sequencer
     *
     * @param sequence
     */
    void resetSequence(Sequence sequence);

    /**
     * get the next sequence
     *
     * @return
     */
    Sequence nextSequence();
}
