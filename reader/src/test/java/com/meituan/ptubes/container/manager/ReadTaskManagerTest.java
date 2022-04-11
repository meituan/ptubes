package com.meituan.ptubes.container.manager;

import com.meituan.ptubes.container.simulator.MockedReplicatorEventProducer;
import com.meituan.ptubes.container.simulator.TestConstant;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.container.simulator.MockedReadTaskManager;
import com.meituan.ptubes.container.simulator.factory.ProducerConfigFactory;
import com.meituan.ptubes.container.simulator.factory.StorageConfigFactory;
import com.meituan.ptubes.reader.container.ReaderTask;
import com.meituan.ptubes.reader.monitor.vo.ReaderRuntimeInfo;
import org.junit.Assert;
import org.junit.Test;

public class ReadTaskManagerTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReadTaskManagerTest.class);

    private static final long SLEEP_MS = 1000L;

    public void init() {
        System.setProperty("user.dir", ReadTaskManagerTest.class.getResource("/").getPath());
    }

    @Test
    public void fetchRuntimeConfTest() {
        try {
            init();
            MockedReadTaskManager readTaskManager = new MockedReadTaskManager(null, null);
            MockedReplicatorEventProducer eventProducer = new MockedReplicatorEventProducer(
                    ProducerConfigFactory.newProducerConfig(),
                    StorageConfigFactory.newStorageConfig(),
                    null
            );
            readTaskManager.getReaderTasks().put(eventProducer.getReaderTaskName(), new ReaderTask(eventProducer.getReaderTaskName(), eventProducer));
            eventProducer.setMockIsRunning(true);
            ReaderRuntimeInfo readerRuntimeInfo = readTaskManager.fetchRuntimeConf(TestConstant.READER_TASK_NAME);
            Assert.assertNotNull(readerRuntimeInfo);
            Assert.assertEquals(TestConstant.READER_TASK_NAME, readerRuntimeInfo.getReaderTaskName());
            Assert.assertEquals(TestConstant.READER_TASK_RDS_USER_NAME, readerRuntimeInfo.getProducerConfig().getRdsConfig().getUserName());
            Assert.assertEquals(TestConstant.READER_TASK_RDS_PASSWORD, readerRuntimeInfo.getProducerConfig().getRdsConfig().getPassword());
        } catch (Exception e) {
            LOG.error("e", e);
            e.printStackTrace();
        }
    }

    @Test
    public void fetchRuntimeConfNoneProducerTest() {
        try {
            init();
            MockedReadTaskManager readTaskManager = new MockedReadTaskManager(null, null);
            MockedReplicatorEventProducer eventProducer = new MockedReplicatorEventProducer(
                    ProducerConfigFactory.newProducerConfig(),
                    StorageConfigFactory.newStorageConfig(),
                    null
            );
            readTaskManager.getReaderTasks().put(eventProducer.getReaderTaskName(), new ReaderTask(eventProducer.getReaderTaskName(), eventProducer));
            eventProducer.setMockIsRunning(false);
            ReaderRuntimeInfo readerRuntimeInfo = readTaskManager.fetchRuntimeConf(TestConstant.READER_TASK_NAME);
            Assert.assertNull(readerRuntimeInfo);
        } catch (Exception e) {
            LOG.error("e", e);
            e.printStackTrace();
        }
    }

    @Test
    public void fetchRuntimeConfNotRunningTest() {
        try {
            init();
            MockedReadTaskManager readTaskManager = new MockedReadTaskManager(null, null);
            ReaderRuntimeInfo readerRuntimeInfo = readTaskManager.fetchRuntimeConf(TestConstant.READER_TASK_NAME);
            Assert.assertNull(readerRuntimeInfo);
        } catch (Exception e) {
            LOG.error("e", e);
            e.printStackTrace();
        }
    }

    @Test
    public void fetchRuntimeConfLockTest() {
        try {
            init();
            MockedReadTaskManager readTaskManager = new MockedReadTaskManager(null, null);
            MockedReplicatorEventProducer eventProducer = new MockedReplicatorEventProducer(
                    ProducerConfigFactory.newProducerConfig(),
                    StorageConfigFactory.newStorageConfig(),
                    null
            );
            readTaskManager.getReaderTasks().put(eventProducer.getReaderTaskName(), new ReaderTask(eventProducer.getReaderTaskName(), eventProducer));
            eventProducer.setMockIsRunning(true);
            long time1 = System.currentTimeMillis();
            new LockObjectThread(eventProducer).start();
            Thread.sleep(500);
            ReaderRuntimeInfo readerRuntimeInfo = readTaskManager.fetchRuntimeConf(TestConstant.READER_TASK_NAME);
            long time2 = System.currentTimeMillis();
            Assert.assertNotNull(readerRuntimeInfo);
            Assert.assertTrue((time2 - time1) > SLEEP_MS);
        } catch (Exception e) {
            LOG.error("e", e);
            e.printStackTrace();
        }
    }

    static class LockObjectThread extends Thread {
        private final Object toLock;

        @Override
        public void run() {
            synchronized (toLock) {
                try {
                    Thread.sleep(SLEEP_MS);
                } catch (InterruptedException e) {
                    LOG.error("LockObjectThread error", e);
                }
            }
        }

        public LockObjectThread(Object toLock) {
            this.toLock = toLock;
        }
    }
}
