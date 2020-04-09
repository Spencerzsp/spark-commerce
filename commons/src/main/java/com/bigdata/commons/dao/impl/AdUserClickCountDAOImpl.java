package com.bigdata.commons.dao.impl;

import com.bigdata.commons.bean.AdUserClickCountQueryResult;
import com.bigdata.commons.dao.IAdUserClickCountDAO;
import com.bigdata.commons.model.AdUserClickCount;
import com.bigdata.commons.utils.JDBCHelper;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO , Serializable {
    @Override
    public void updateBatch(List<AdUserClickCount> adUserClickCounts) {

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        //首先将用户广告点击量进行分类，分成待插入和已插入
        List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<AdUserClickCount>();
        List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<AdUserClickCount>();

        // 查询用户点某天点击某个广告的次数
        String selectSQL = "select count(*) from ad_user_click_count where date=? and userid=? and adid=?";
        Object[] selectParam = null;

        for (AdUserClickCount adUserClickCount : adUserClickCounts) {
            AdUserClickCountQueryResult adUserClickCountQueryResult = new AdUserClickCountQueryResult();
            selectParam = new Object[]{
              adUserClickCount.date(),
              adUserClickCount.userid(),
              adUserClickCount.adid()
            };

            jdbcHelper.executeQuery(selectSQL, selectParam, new JDBCHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()){
                        int count = rs.getInt(1);
                        adUserClickCountQueryResult.setCount(count);
                    }
                }
            });

            int count = adUserClickCountQueryResult.getCount();
            if (count > 0){
                updateAdUserClickCounts.add(adUserClickCount);
            }else {
                insertAdUserClickCounts.add(adUserClickCount);
            }
        }

        // 执行批量插入
        String insertSQL = "insert into ad_user_click_count values(?,?,?,?)";

        List<Object[]> insertParamList  = new ArrayList<>();
        Object[] insertParam = null;
        for (AdUserClickCount insertAdUserClickCount : insertAdUserClickCounts) {
            insertParam = new Object[]{
                insertAdUserClickCount.date(),
                insertAdUserClickCount.userid(),
                insertAdUserClickCount.adid(),
                insertAdUserClickCount.clickCount()
            };
            insertParamList.add(insertParam);
        }

        jdbcHelper.executeBatch(insertSQL, insertParamList);

        // 执行批量更新操作
        String updateSQL = "update ad_user_click_count set clickCount = ? where date = ? and adid = ? and userid = ?";

        List<Object[]> updateParamList = new ArrayList<>();
        Object[] updateParam = null;
        for (AdUserClickCount updateAdUserClickCount : updateAdUserClickCounts) {
            updateParam = new Object[]{
              updateAdUserClickCount.clickCount(),
                    updateAdUserClickCount.date(),
                    updateAdUserClickCount.adid(),
                    updateAdUserClickCount.userid()
            };

            updateParamList.add(updateParam);
        }

        jdbcHelper.executeBatch(updateSQL, updateParamList);
    }

    @Override
    public int findClickCountByMultiKey(String date, long userid, long adid) {

        String sql = "select clickCount from ad_user_click_count where date = ? and userid = ? and adid = ?";
        Object[] params = new Object[]{date, userid, adid};
        AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()){
                    int clickCount = rs.getInt(1);
                    queryResult.setCount(clickCount);
                }
            }
        });

        int clickCount = queryResult.getCount();

        return clickCount;
    }
}
