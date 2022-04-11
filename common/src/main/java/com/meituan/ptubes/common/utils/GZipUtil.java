package com.meituan.ptubes.common.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class GZipUtil {

    public static final int BUFFER = 64 * 1024;
    public static final String EXT = ".gz";

    public static boolean isGZip(File file) throws FileNotFoundException {
        InputStream is = new FileInputStream(file);
        byte[] signature = new byte[2];
        try {
            int readable = is.read(signature);
            if (readable != 2) {
                return false;
            }
            return signature[0] == (byte) 0x1f && signature[1] == (byte) 0x8b;
        } catch (IOException io) {
            return false;
        } finally {
            try {
                is.close();
            } catch (IOException ignore) {
            }
        }
    }

    public static byte[] compress(byte[] data) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            // compression
            compress(
                bais,
                baos
            );

            byte[] output = baos.toByteArray();
            baos.flush();
            return output;
        } finally {
            baos.close();
            bais.close();
        }
    }

    public static void compress(
        File file,
        boolean delete
    ) throws Exception {
        FileInputStream fis = new FileInputStream(file);
        FileOutputStream fos = new FileOutputStream(file.getPath() + EXT);

        try {
            compress(
                fis,
                fos
            );
            fos.flush();
            if (delete) {
                file.delete();
            }
        } finally {
            fis.close();
            fos.close();
        }

    }

    private static void compress(
        InputStream is,
        OutputStream os
    )
        throws Exception {

        GZIPOutputStream gos = new GZIPOutputStream(os);

        try {
            int count;
            byte[] data = new byte[BUFFER];
            while ((count = is.read(data,
                                    0,
                                    BUFFER
            )) != -1) {
                gos.write(
                    data,
                    0,
                    count
                );
            }
            gos.finish();
            gos.flush();
        } finally {
            gos.close();
        }
    }

    public static void compress(
        String path,
        boolean delete
    ) throws Exception {
        File file = new File(path);
        compress(
            file,
            delete
        );
    }

    public static byte[] decompress(byte[] data) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // unzip

        try {
            decompress(
                bais,
                baos
            );
            data = baos.toByteArray();
            baos.flush();
            return data;
        } finally {
            baos.close();
            bais.close();
        }
    }

    public static void decompress(
        File file,
        boolean delete
    ) throws Exception {
        String targetFilePath = file.getPath()
            .replace(
                EXT,
                ""
            );
        String tempFilePath = targetFilePath + "." + System.nanoTime();
        File tempFile = new File(tempFilePath);
        File targetFile = new File(targetFilePath);
        if (targetFile.exists()) {
            return;
        }

        FileInputStream fis = new FileInputStream(file);
        FileOutputStream fos = new FileOutputStream(tempFilePath);

        try {
            decompress(
                fis,
                fos
            );
            fos.flush();

            boolean renameResult = tempFile.renameTo(targetFile);
            if (renameResult == false) {
                tempFile.delete();
            } else if (delete && targetFile.exists()) {
                file.delete();
            }
        } finally {
            fis.close();
            fos.close();
        }
    }

    public static void decompress(
        InputStream is,
        OutputStream os
    )
        throws Exception {

        GZIPInputStream gis = new GZIPInputStream(is);

        try {
            int count;
            byte[] data = new byte[BUFFER];
            while ((count = gis.read(data,
                                     0,
                                     BUFFER
            )) != -1) {
                os.write(
                    data,
                    0,
                    count
                );
            }
        } finally {
            gis.close();
        }
    }

}
