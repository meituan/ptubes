package com.meituan.ptubes.reader.container.network.request.monitor;

import java.util.List;
import com.meituan.ptubes.reader.container.common.vo.RedefinedToString;

public class RuntimeConfRequest extends RedefinedToString {

    private List<String> taskName;

    /**
     * query all tasks
     */
    private Boolean allTask;

    public List<String> getTaskName() {
        return taskName;
    }

    public void setTaskName(List<String> taskName) {
        this.taskName = taskName;
    }

    public Boolean getAllTask() {
        return allTask;
    }

    public void setAllTask(Boolean allTask) {
        this.allTask = allTask;
    }
}
