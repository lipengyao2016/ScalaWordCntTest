package com.bigdata;

import com.bigdata.bean.WordCntResultDo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.*;


public class FlinkToMySqlResult extends RichSinkFunction<WordCntResultDo> {

    private Connection conn;
    private PreparedStatement qsSt;
    private PreparedStatement insertSt;
    private PreparedStatement updateSt;

    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test" +
                        "?characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
                "root",
                "123456");
        qsSt = conn.prepareStatement(" select id from word_cnt_result where `name` = ? ");
        insertSt = conn.prepareStatement(" insert into word_cnt_result(`name`,cnt,update_time) values(?, ?, ?) ");
        updateSt = conn.prepareStatement(" update word_cnt_result  set cnt = ? ,update_time = ? where `name` = ? ");

    }

    protected int getResByName(String name)
    {
        try {
            qsSt.setString(1,name);
            ResultSet res =qsSt.executeQuery();
            while (res.next())
            {
                   return res.getInt("id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return  0 ;

    }

    protected int updateCnt(WordCntResultDo wordCntResultDo)
    {
        try {
             updateSt.setInt(1,wordCntResultDo.getCnt());
            updateSt.setTimestamp(2,new Timestamp(new java.util.Date().getTime()));
             updateSt.setString(3,wordCntResultDo.getName());
            int updateRet = updateSt.executeUpdate();
            System.out.println(" updateRet:" + updateRet);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return  0 ;

    }

    protected int addCnt(WordCntResultDo wordCntResultDo)
    {
        try {
            insertSt.setInt(2,wordCntResultDo.getCnt());
            insertSt.setTimestamp(3,new Timestamp(new java.util.Date().getTime()));
            insertSt.setString(1,wordCntResultDo.getName());
            int insertRet = insertSt.executeUpdate();
            System.out.println(" insertRet:" + insertRet);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return  0 ;

    }

    @Override
    public void invoke(WordCntResultDo value, SinkFunction.Context context) throws Exception {

        System.out.println(" value:" + value);
        if (StringUtils.isNotEmpty(value.getName()))
        {
            int id = getResByName(value.getName());
            if (id > 0)
            {
                updateCnt(value);
            }
            else{
                addCnt(value);
            }
        }



    }

    public void close() throws Exception {
        qsSt.close();
        insertSt.close();
        updateSt.close();
    }
}
