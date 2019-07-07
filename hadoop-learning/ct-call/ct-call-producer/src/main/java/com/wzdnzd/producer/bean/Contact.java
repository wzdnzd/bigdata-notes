/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.producer.bean;

import com.wzdnzd.bean.DataObject;
import com.wzdnzd.constant.ConstantVal;

public class Contact extends DataObject {
    private String phoneNumber;
    private String username;

    public Contact() {
    }

    public Contact(String phoneNumber, String username) {
        this.phoneNumber = phoneNumber;
        this.username = username;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    private void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getUsername() {
        return username;
    }

    private void setUsername(String username) {
        this.username = username;
    }

    @Override
    public void setVal(Object val) {
        String[] contents = ((String) val).split(ConstantVal.DELIMITER.getVal());
        this.setPhoneNumber(contents[0]);
        this.setUsername(contents[1]);
    }

    @Override
    public String toString() {
        return "Contact{" +
                "phoneNumber='" + phoneNumber + '\'' +
                ", username='" + username + '\'' +
                '}';
    }
}
