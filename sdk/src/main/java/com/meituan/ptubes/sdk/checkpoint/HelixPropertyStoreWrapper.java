package com.meituan.ptubes.sdk.checkpoint;

import com.meituan.ptubes.sdk.constants.ConsumeConstants;
import java.util.List;

import com.meituan.ptubes.sdk.utils.CommonUtil;
import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import com.meituan.ptubes.common.log.Logger;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.zookeeper.data.Stat;

public class HelixPropertyStoreWrapper implements HelixPropertyStore<ZNRecord> {
    private HelixPropertyStore<ZNRecord> propertyStoreAgent;
    private long sleepMs;
    private final Logger log;

    public HelixPropertyStoreWrapper(HelixPropertyStore<ZNRecord> propertyStoreAgent, Logger log) {
        this.propertyStoreAgent = propertyStoreAgent;
        this.log = log;
        this.sleepMs = ConsumeConstants.HELIX_PROPERTY_STORE_RETRY_INTERVAL_MS;
    }

    public HelixPropertyStoreWrapper(HelixPropertyStore<ZNRecord> propertyStoreAgent, Logger log, long sleepMs) {
        this.propertyStoreAgent = propertyStoreAgent;
        this.log = log;
        this.sleepMs = sleepMs;
    }

    @Override
    public void start() {
        propertyStoreAgent.start();
    }

    @Override
    public void stop() {
        propertyStoreAgent.stop();
    }

    @Override
    public void subscribe(String parentPath, HelixPropertyListener listener) {
        propertyStoreAgent.subscribe(parentPath, listener);
    }

    @Override
    public void unsubscribe(String parentPath, HelixPropertyListener listener) {
        propertyStoreAgent.unsubscribe(parentPath, listener);
    }

    @Override
    public boolean create(String path, ZNRecord record, int options) {
        return propertyStoreAgent.create(path, record, options);
    }

    @Override
    public boolean set(String path, ZNRecord record, int options) {
        return propertyStoreAgent.set(path, record, options);
    }

    @Override
    public boolean set(String path, ZNRecord record, int expectVersion, int options) {
        return propertyStoreAgent.set(path, record, expectVersion, options);
    }

    @Override
    public boolean update(String path, DataUpdater<ZNRecord> updater, int options) {
        return propertyStoreAgent.update(path, updater, options);
    }

    @Override
    public boolean remove(String path, int options) {
        return propertyStoreAgent.remove(path, options);
    }

    @Override
    public boolean[] createChildren(List<String> paths, List<ZNRecord> records, int options) {
        return propertyStoreAgent.createChildren(paths, records, options);

    }

    @Override
    public boolean[] setChildren(List<String> paths, List<ZNRecord> records, int options) {
        return propertyStoreAgent.setChildren(paths, records, options);
    }

    @Override
    public boolean[] updateChildren(List<String> paths, List<DataUpdater<ZNRecord>> dataUpdaters, int options) {
        return propertyStoreAgent.updateChildren(paths, dataUpdaters, options);
    }

    @Override
    public boolean[] remove(List<String> paths, int options) {
        return propertyStoreAgent.remove(paths, options);
    }

    @Override
    public ZNRecord get(String path, Stat stat, int options) {
        return propertyStoreAgent.get(path, stat, options);
    }

    @Override
    public List<ZNRecord> get(List<String> paths, List<Stat> stats, int options) {
        return propertyStoreAgent.get(paths, stats, options);
    }

    @Override
    public List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options) {
        return propertyStoreAgent.getChildren(parentPath, stats, options);
    }

    @Override
    public List<String> getChildNames(String parentPath, int options) {
        return propertyStoreAgent.getChildNames(parentPath, options);
    }

    @Override
    public boolean exists(String path, int options) {
        while (true) {
            try {
                return propertyStoreAgent.exists(path, options);
            } catch (Exception e) {
                log.error("fail to invoke propertyStoreAgent.exists, will retry. param:" + path);
                CommonUtil.sleep(this.sleepMs);
            }
        }
    }

    @Override
    public boolean[] exists(List<String> paths, int options) {
        return propertyStoreAgent.exists(paths, options);
    }

    @Override
    public Stat[] getStats(List<String> paths, int options) {
        return propertyStoreAgent.getStats(paths, options);
    }

    @Override
    public Stat getStat(String path, int options) {
        return propertyStoreAgent.getStat(path, options);
    }

    @Override
    public void subscribeDataChanges(String path, IZkDataListener listener) {
        propertyStoreAgent.subscribeDataChanges(path, listener);
    }

    @Override
    public void unsubscribeDataChanges(String path, IZkDataListener listener) {
        propertyStoreAgent.unsubscribeDataChanges(path, listener);
    }

    @Override
    public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
        return propertyStoreAgent.subscribeChildChanges(path, listener);
    }

    @Override
    public void unsubscribeChildChanges(String path, IZkChildListener listener) {
        propertyStoreAgent.unsubscribeChildChanges(path, listener);
    }

    @Override
    public void reset() {
        propertyStoreAgent.reset();
    }
}
