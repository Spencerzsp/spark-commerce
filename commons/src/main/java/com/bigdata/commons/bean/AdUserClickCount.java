package com.bigdata.commons.bean;

/**
 * @ author spencer
 * @ date 2020/6/2 11:05
 */
public class AdUserClickCount {

//    date: String,
//    userid: Long,
//    adid: Long,
//    clickCount: Long
    private String date;
    private Long userid;
    private Long adid;
    private Long clickCount;

    @Override
    public String toString() {
        return "AdUserClickCount{" +
                "date='" + date + '\'' +
                ", userid=" + userid +
                ", adid=" + adid +
                ", clickCount=" + clickCount +
                '}';
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Long getUserid() {
        return userid;
    }

    public void setUserid(Long userid) {
        this.userid = userid;
    }

    public Long getAdid() {
        return adid;
    }

    public void setAdid(Long adid) {
        this.adid = adid;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }
}
