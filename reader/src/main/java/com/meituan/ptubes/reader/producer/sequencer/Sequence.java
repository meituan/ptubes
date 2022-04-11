package com.meituan.ptubes.reader.producer.sequencer;

public class Sequence {

    // monotonically increasing id for every binlog row
    private long sequenceId;

    public static Sequence of(long sequenceId) {
        Sequence sequence = new Sequence();
        sequence.setSequenceId(sequenceId);
        return sequence;
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
    }
}
