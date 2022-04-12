package com.meituan.ptubes.storage;

import com.meituan.ptubes.common.utils.DateUtil;
import com.meituan.ptubes.common.utils.GZipUtil;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import com.meituan.ptubes.storage.utils.FileUtil;
import java.io.File;
import java.io.IOException;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.data.DataManagerFinder;
import com.meituan.ptubes.reader.storage.file.data.read.SingleReadDataManager;
import org.junit.Assert;
import org.junit.Test;



public class FileRetentionAndCompressTest {

    @Test
    public void testFileRetentionWithoutRead() throws Exception {
        String readerTaskName = "testFileRetentionWithoutRead";
        int retentionHours = 1;

        File baseDir = new File(FileSystem.DEFAULT_PATH + "/" + readerTaskName);
        if (baseDir.exists()) {
            FileUtil.delFile(baseDir);
        }

        long currentTs = System.currentTimeMillis();
        int currentHourInteger = DateUtil.getDateHour(currentTs);
        createFileByHourTime(
            readerTaskName,
            currentHourInteger
        );

        int lastOneHour = DateUtil.getDateHour(currentTs - 3600 * 1000);
        createFileByHourTime(
            readerTaskName,
            lastOneHour
        );

        int lastTwoHour = DateUtil.getDateHour(currentTs - 2 * 3600 * 1000);
        createFileByHourTime(
            readerTaskName,
            lastTwoHour
        );

        FileSystem.retentionFile(
            readerTaskName,
            retentionHours
        );

        long nowTx = System.currentTimeMillis();
        int nowHour = DateUtil.getDateHour(nowTx);

        checkDirExpired(
            readerTaskName,
            lastOneHour
        );
        checkDirExpired(
            readerTaskName,
            lastTwoHour
        );

        if (nowHour == currentHourInteger) {
            checkDirNotExpired(
                readerTaskName,
                currentHourInteger
            );
        }
    }

    @Test
    public void testFileRetentionWithRead() throws Exception {
        String readerTaskName = "testFileRetentionWithRead";
        int retentionHours = 1;

        File baseDir = new File(FileSystem.DEFAULT_PATH + "/" + readerTaskName);
        if (baseDir.exists()) {
            FileUtil.delFile(baseDir);
        }

        long currentTs = System.currentTimeMillis();
        int currentHourInteger = DateUtil.getDateHour(currentTs);
        createFileByHourTime(
            readerTaskName,
            currentHourInteger
        );

        int lastOneHour = DateUtil.getDateHour(currentTs - 3600 * 1000);
        createFileByHourTime(
            readerTaskName,
            lastOneHour
        );

        int lastTwoHour = DateUtil.getDateHour(currentTs - 2 * 3600 * 1000);
        createFileByHourTime(
            readerTaskName,
            lastTwoHour
        );

        // lastTwoHour is reading
        SingleReadDataManager readDataManager = DataManagerFinder.findReadDataManager(
            readerTaskName,
            new StorageConfig.FileConfig(),
            new DataPosition(
                lastTwoHour,
                0,
                0L
            )
        );

        FileSystem.retentionFile(
            readerTaskName,
            retentionHours
        );

        long nowTx = System.currentTimeMillis();
        int nowHour = DateUtil.getDateHour(nowTx);

        checkDirNotExpired(
            readerTaskName,
            lastOneHour
        );
        checkDirNotExpired(
            readerTaskName,
            lastTwoHour
        );
        if (nowHour == currentHourInteger) {
            checkDirNotExpired(
                readerTaskName,
                currentHourInteger
            );
        }

        // lastOneHour is reading
        readDataManager.stop();
        readDataManager = DataManagerFinder.findReadDataManager(
            readerTaskName,
            new StorageConfig.FileConfig(),
            new DataPosition(
                lastOneHour,
                0,
                0L
            )
        );

        FileSystem.retentionFile(
            readerTaskName,
            retentionHours
        );

        nowTx = System.currentTimeMillis();
        nowHour = DateUtil.getDateHour(nowTx);

        checkDirNotExpired(
            readerTaskName,
            lastOneHour
        );
        checkDirExpired(
            readerTaskName,
            lastTwoHour
        );
        if (nowHour == currentHourInteger) {
            checkDirNotExpired(
                readerTaskName,
                currentHourInteger
            );
        }

        // not read
        readDataManager.stop();

        FileSystem.retentionFile(
            readerTaskName,
            retentionHours
        );

        nowTx = System.currentTimeMillis();
        nowHour = DateUtil.getDateHour(nowTx);

        checkDirExpired(
            readerTaskName,
            lastOneHour
        );
        if (nowHour == currentHourInteger) {
            checkDirNotExpired(
                readerTaskName,
                currentHourInteger
            );
        }
    }

    @Test
    public void testFileCompressWithoutRead() throws Exception {
        String readerTaskName = "testFileCompressWithoutRead";
        int retentionHours = 1;

        File baseDir = new File(FileSystem.DEFAULT_PATH + "/" + readerTaskName);
        if (baseDir.exists()) {
            FileUtil.delFile(baseDir);
        }

        long currentTs = System.currentTimeMillis();
        int currentHourInteger = DateUtil.getDateHour(currentTs);
        createFileByHourTime(
            readerTaskName,
            currentHourInteger
        );

        int lastOneHour = DateUtil.getDateHour(currentTs - 3600 * 1000);
        createFileByHourTime(
            readerTaskName,
            lastOneHour
        );

        int lastTwoHour = DateUtil.getDateHour(currentTs - 2 * 3600 * 1000);
        createFileByHourTime(
            readerTaskName,
            lastTwoHour
        );

        FileSystem.compressFile(
            readerTaskName,
            retentionHours
        );

        long nowTx = System.currentTimeMillis();
        int nowHour = DateUtil.getDateHour(nowTx);

        checkDirCompressed(
            readerTaskName,
            lastOneHour
        );
        checkDirCompressed(
            readerTaskName,
            lastTwoHour
        );

        if (nowHour == currentHourInteger) {
            checkDirNotCompressed(
                readerTaskName,
                currentHourInteger
            );
        }
    }

    @Test
    public void testFileCompressWithRead() throws Exception {
        String readerTaskName = "testFileCompressWithRead";
        int retentionHours = 1;

        File baseDir = new File(FileSystem.DEFAULT_PATH + "/" + readerTaskName);
        if (baseDir.exists()) {
            FileUtil.delFile(baseDir);
        }

        long currentTs = System.currentTimeMillis();
        int currentHourInteger = DateUtil.getDateHour(currentTs);
        createFileByHourTime(
            readerTaskName,
            currentHourInteger
        );

        int lastOneHour = DateUtil.getDateHour(currentTs - 3600 * 1000);
        createFileByHourTime(
            readerTaskName,
            lastOneHour
        );

        int lastTwoHour = DateUtil.getDateHour(currentTs - 2 * 3600 * 1000);
        createFileByHourTime(
            readerTaskName,
            lastTwoHour
        );

        // lastTwoHour is reading
        SingleReadDataManager readDataManager = DataManagerFinder.findReadDataManager(
            readerTaskName,
            new StorageConfig.FileConfig(),
            new DataPosition(
                lastTwoHour,
                0,
                0L
            )
        );

        FileSystem.compressFile(
            readerTaskName,
            retentionHours
        );

        long nowTx = System.currentTimeMillis();
        int nowHour = DateUtil.getDateHour(nowTx);

        checkDirNotCompressed(
            readerTaskName,
            lastOneHour
        );
        checkDirNotCompressed(
            readerTaskName,
            lastTwoHour
        );

        if (nowHour == currentHourInteger) {
            checkDirNotCompressed(
                readerTaskName,
                currentHourInteger
            );
        }

        // lastOneHour is reading
        readDataManager.stop();
        readDataManager = DataManagerFinder.findReadDataManager(
            readerTaskName,
            new StorageConfig.FileConfig(),
            new DataPosition(
                lastOneHour,
                0,
                0L
            )
        );

        FileSystem.compressFile(
            readerTaskName,
            retentionHours
        );

        nowTx = System.currentTimeMillis();
        nowHour = DateUtil.getDateHour(nowTx);

        checkDirNotCompressed(
            readerTaskName,
            lastOneHour
        );
        checkDirCompressed(
            readerTaskName,
            lastTwoHour
        );
        if (nowHour == currentHourInteger) {
            checkDirNotCompressed(
                readerTaskName,
                currentHourInteger
            );
        }

        // not read
        readDataManager.stop();

        FileSystem.compressFile(
            readerTaskName,
            retentionHours
        );

        nowTx = System.currentTimeMillis();
        nowHour = DateUtil.getDateHour(nowTx);

        checkDirCompressed(
            readerTaskName,
            lastOneHour
        );
        if (nowHour == currentHourInteger) {
            checkDirNotCompressed(
                readerTaskName,
                currentHourInteger
            );
        }
    }

    private void createFileByHourTime(
        String readerTaskName,
        int hour
    ) throws IOException {
        // create data
        File dataFile0 = new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.dataDir + hour + "/bucket-0.data");
        FileUtil.createFile(dataFile0);

        // create index
        File l1IndexFile0 = new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.l1IndexDir + "/l1Index.l1idx");
        FileUtil.createFile(l1IndexFile0);

        File l2IndexFile0 = new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.l2IndexDir + hour + "/bucket-0.l2idx");
        FileUtil.createFile(l2IndexFile0);
    }

    private void checkDirExpired(
        String readerTaskName,
        int hour
    ) {
        Assert.assertFalse(new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.dataDir + hour).exists());
        Assert.assertTrue(new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.STORAGE_DIR_NAME + "/expired/" +
                hour +
                FileSystem.DATA_SUFFIX).exists());
        Assert.assertTrue(new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.STORAGE_DIR_NAME + "/expired/" +
                hour +
                FileSystem.L2_INDEX_SUFFIX).exists());
    }

    private void checkDirNotExpired(
        String readerTaskName,
        int hour
    ) {
        Assert.assertFalse(new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.STORAGE_DIR_NAME + "/expired/" +
                hour +
                FileSystem.DATA_SUFFIX).exists());
        Assert.assertTrue(new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.dataDir +
                hour).exists());
    }

    private void checkDirCompressed(
        String readerTaskName,
        int hour
    ) {
        File dataFile = new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.dataDir + hour + "/bucket-0.data");
        Assert.assertFalse(dataFile.exists());
        File dataFileCompressed = new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.dataDir + hour + "/bucket-0.data" +
                GZipUtil.EXT);

        Assert.assertTrue(dataFileCompressed.exists());

        Assert.assertFalse(new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.l1IndexDir + "/l1Index.l1idx" +
                GZipUtil.EXT).exists());
        Assert.assertFalse(new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.STORAGE_DIR_NAME + "/expired/" +
                hour +
                FileSystem.L2_INDEX_SUFFIX + GZipUtil.EXT).exists());
    }

    private void checkDirNotCompressed(
        String readerTaskName,
        int hour
    ) {
        File dataFile = new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.dataDir + hour + "/bucket-0.data");
        Assert.assertTrue(dataFile.exists());
        File dataFileCompressed = new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.dataDir + hour + "/bucket-0.data" +
                GZipUtil.EXT);
        Assert.assertFalse(dataFileCompressed.exists());
        Assert.assertFalse(new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.l1IndexDir + "/l1Index.l1idx" +
                GZipUtil.EXT).exists());
        Assert.assertFalse(new File(
            FileSystem.DEFAULT_PATH + "/" + readerTaskName + "/" + FileSystem.STORAGE_DIR_NAME + "/expired/" +
                hour +
                FileSystem.L2_INDEX_SUFFIX + GZipUtil.EXT).exists());
    }

}
