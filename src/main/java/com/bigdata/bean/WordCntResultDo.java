package com.bigdata.bean;

import scala.Int;

import java.io.Serializable;
import java.util.Date;

public class WordCntResultDo implements Serializable {


    private Integer id;

    private String name;

    private Integer cnt;

    private Date update_time;

    public WordCntResultDo(String name, Integer cnt) {
        this.name = name;
        this.cnt = cnt;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getCnt() {
        return cnt;
    }

    public void setCnt(Integer cnt) {
        this.cnt = cnt;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }


    @Override
    public String toString() {
        return "WordCntResultDo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", cnt=" + cnt +
                ", update_time=" + update_time +
                '}';
    }
}
