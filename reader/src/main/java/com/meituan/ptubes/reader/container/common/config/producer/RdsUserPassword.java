package com.meituan.ptubes.reader.container.common.config.producer;


public class RdsUserPassword {

    private String userName;
    private String password;

    public RdsUserPassword(
        String userName,
        String password
    ) {
        this.userName = userName;
        this.password = password;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
