package tech.dingxin.maxcompute.entity;

import javax.xml.bind.annotation.XmlElement;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */


public class Job {
    private int priority;
    private Tasks tasks;

    @XmlElement(name = "Priority")
    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @XmlElement(name = "Tasks")
    public Tasks getTasks() {
        return tasks;
    }

    public void setTasks(Tasks tasks) {
        this.tasks = tasks;
    }
}
