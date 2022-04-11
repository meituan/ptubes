package com.meituan.ptubes.sdk.config.notification;

import java.nio.charset.StandardCharsets;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class DefaultZkUTF8Serializer implements ZkSerializer {
    @Override
    public byte[] serialize(Object object) throws ZkMarshallingError {
        return ((String)object).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
