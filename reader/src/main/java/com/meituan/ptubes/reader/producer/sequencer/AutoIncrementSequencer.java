package com.meituan.ptubes.reader.producer.sequencer;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class AutoIncrementSequencer implements Sequencer {

    protected static final Logger LOG = LoggerFactory.getLogger(AutoIncrementSequencer.class);

    private AtomicLong sequenceId = new AtomicLong(0L);

    @Override
    public void resetSequence(Sequence sequence) {
        if (Objects.isNull(sequence)) {
            throw new IllegalArgumentException("sequence is null.");
        }

        long oldSequenceId = sequenceId.get();
        sequenceId.set(sequence.getSequenceId());
        LOG.info("resetSequence success, oldSequenceId={}, newSequenceId={}", oldSequenceId, sequenceId);
    }

    @Override
    public Sequence nextSequence() {
        return Sequence.of(sequenceId.incrementAndGet());
    }
}
