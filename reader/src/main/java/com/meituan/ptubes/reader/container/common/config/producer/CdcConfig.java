package com.meituan.ptubes.reader.container.common.config.producer;

public class CdcConfig {

    private int version;
    private int partitionId;
    private CdcInstance cdcInstance;
    private PdInstance pdInstance;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public CdcInstance getCdcInstance() {
        return cdcInstance;
    }

    public void setCdcInstance(CdcInstance cdcInstance) {
        this.cdcInstance = cdcInstance;
    }

    public PdInstance getPdInstance() {
        return pdInstance;
    }

    public void setPdInstance(PdInstance pdInstance) {
        this.pdInstance = pdInstance;
    }

    // json not support non-static class because of internal class cannot initialize before external class
    public static class CdcInstance {
        private String host;
        private int port;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    public static class PdInstance {
        private String host;
        private int port;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }
}
