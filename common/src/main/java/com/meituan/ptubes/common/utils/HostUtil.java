package com.meituan.ptubes.common.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;



public class HostUtil {

    public static String getLocalHostName() {
        try {
            return InetAddress.getLocalHost()
                .getCanonicalHostName();
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
