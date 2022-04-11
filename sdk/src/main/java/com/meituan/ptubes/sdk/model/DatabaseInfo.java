package com.meituan.ptubes.sdk.model;

import java.util.Objects;
import java.util.Set;



public class DatabaseInfo {
    private String databaseName;
    private Set<String> tableNames;

    public DatabaseInfo() {

    }

    public DatabaseInfo(
        String databaseName,
        Set<String> tableNames
    ) {
        this.databaseName = databaseName;
        this.tableNames = tableNames;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Set<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(Set<String> tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseInfo that = (DatabaseInfo) o;
        return Objects.equals(
            databaseName,
            that.databaseName
        ) &&
            Objects.equals(
                tableNames,
                that.tableNames
            );
    }

    @Override
    public int hashCode() {

        return Objects.hash(
            databaseName,
            tableNames
        );
    }
}
