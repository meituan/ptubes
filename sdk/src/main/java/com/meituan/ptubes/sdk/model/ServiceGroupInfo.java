package com.meituan.ptubes.sdk.model;

import java.util.Objects;
import java.util.Set;



public class ServiceGroupInfo {

    private String taskName;

    private String serviceGroupName;

    private Set<DatabaseInfo> databaseInfoSet;

    public ServiceGroupInfo() {

    }

    public ServiceGroupInfo(String taskName, String serviceGroupName, Set<DatabaseInfo> databaseInfoSet) {
        this.taskName = taskName;
        this.serviceGroupName = serviceGroupName;
        this.databaseInfoSet = databaseInfoSet;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getServiceGroupName() {
        return serviceGroupName;
    }

    public Set<DatabaseInfo> getDatabaseInfoSet() {
        return databaseInfoSet;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public void setServiceGroupName(String serviceGroupName) {
        this.serviceGroupName = serviceGroupName;
    }

    public void setDatabaseInfoSet(Set<DatabaseInfo> databaseInfoSet) {
        this.databaseInfoSet = databaseInfoSet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServiceGroupInfo that = (ServiceGroupInfo) o;
        return Objects.equals(
            taskName,
            that.taskName
        ) &&
            Objects.equals(
                serviceGroupName,
                that.serviceGroupName
            ) &&
            Objects.equals(
                databaseInfoSet,
                that.databaseInfoSet
            );
    }

    @Override
    public int hashCode() {

        return Objects.hash(
            taskName,
            serviceGroupName,
            databaseInfoSet
        );
    }

    @Override
    public String toString() {
        return "ServiceGroupInfo{" +
            "taskName='" + taskName + '\'' +
            ", serviceGroupName='" + serviceGroupName +
            '}';
    }
}
