package com.meituan.ptubes.reader.container.network.encoder;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.network.cache.BinaryEvent;
import java.util.List;

public interface IEventToByteEncoder {

    byte[] encodePackage(SourceType sourceType, List<BinaryEvent> binaryEvents);

}
