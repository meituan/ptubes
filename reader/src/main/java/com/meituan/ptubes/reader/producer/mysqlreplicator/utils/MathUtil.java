package com.meituan.ptubes.reader.producer.mysqlreplicator.utils;

import com.google.common.base.Preconditions;

public class MathUtil {

    public static int normalizeBufferSize(int bufferSize, int min, int max) {
        Preconditions.checkArgument(bufferSize > min && bufferSize <= max, "buffer size must be in (" + min + ", " + max + "]");

        int normalizedBufferSize = bufferSize;
        normalizedBufferSize--;
        normalizedBufferSize |= normalizedBufferSize >>> 1;
        normalizedBufferSize |= normalizedBufferSize >>> 2;
        normalizedBufferSize |= normalizedBufferSize >>> 4;
        normalizedBufferSize |= normalizedBufferSize >>> 8;
        normalizedBufferSize |= normalizedBufferSize >>> 16;
        normalizedBufferSize++;

        return normalizedBufferSize;
    }

}
