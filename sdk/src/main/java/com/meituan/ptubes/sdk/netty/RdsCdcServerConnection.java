package com.meituan.ptubes.sdk.netty;



public interface RdsCdcServerConnection {
    /** Close the Connection **/
    void close();

    /** The host name or ip address of the remote server */
    String getRemoteHost();

    /** Get a service name identifying the remote server */
    String getRemoteService();

}
