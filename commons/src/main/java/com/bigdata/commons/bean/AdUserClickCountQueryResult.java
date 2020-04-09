package com.bigdata.commons.bean;

public class AdUserClickCountQueryResult {

    private int count;

    @Override
    public String toString() {
        return "AdUserClickCountQueryResult{" +
                "count=" + count +
                '}';
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
