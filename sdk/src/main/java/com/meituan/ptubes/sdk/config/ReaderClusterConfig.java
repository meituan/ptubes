package com.meituan.ptubes.sdk.config;

import com.google.common.collect.Lists;
import com.meituan.ptubes.common.utils.PropertiesUtils;
import com.meituan.ptubes.sdk.model.ReaderServerInfo;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class ReaderClusterConfig {
    private static final String CONFIG_FILE = "ptubes-writer.conf";
    private static final String READER_ADDRESS = "readerAddress";

    private static volatile List<ReaderServerInfo> readerServerInfos;

    public static List<ReaderServerInfo> getReaderCluster(){
        if(Objects.isNull(readerServerInfos)){
            synchronized (ReaderClusterConfig.class){
                if(Objects.isNull(readerServerInfos)){
                    Properties properties = PropertiesUtils.getProperties(CONFIG_FILE);
                    String remoteServerInfoString = properties.getProperty(READER_ADDRESS);
                    String[] servers = remoteServerInfoString.split(",");
                    List<ReaderServerInfo> readerList = Lists.newArrayList();
                    for(String server : servers){
                        String[] serverInfo = server.split(":");
                        ReaderServerInfo readerServerInfo = new ReaderServerInfo();
                        readerServerInfo.setIp(serverInfo[0]);
                        readerServerInfo.setPort(Integer.valueOf(serverInfo[1]));
                        readerList.add(readerServerInfo);
                    }
                    readerServerInfos = readerList;
                }
            }
        }
        return Collections.unmodifiableList(readerServerInfos);
    }
}
