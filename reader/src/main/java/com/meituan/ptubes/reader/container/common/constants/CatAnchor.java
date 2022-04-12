package com.meituan.ptubes.reader.container.common.constants;

public class CatAnchor {
    /**
     * Clustering priority of exception management high -> low writerTask -> clientId -> readerTask
     */
    public enum CatCategoryType {
        OTHER_OR_UNKNOWN("DTS.Reader.Unknown"),
        CLIENT_ID("DTS.Reader.ClientId"),
        CLIENT_TASK_NAME("DTS.Reader.ClientTask"),
        READER_TASK_NAME("DTS.Reader.ReaderTask");

        String categorySeparator;

        CatCategoryType(String categorySeparator) {
            this.categorySeparator = categorySeparator;
        }
    }

    /**
     * Generate cat dotted category
     *
     * @param error
     * @param catCategoryType
     * @param name            According to the business scenario, there are currently three types: clientName, readerName, clientId
     */
    public static String getCatLogCatAnchor(String error, CatCategoryType catCategoryType, String name) {
        if (null == catCategoryType) {
            return CatCategoryType.OTHER_OR_UNKNOWN.categorySeparator + "." + error + "." + name;
        }
        return catCategoryType.categorySeparator + "." + error + "." + name;
    }
}
