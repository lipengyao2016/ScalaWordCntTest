package com.bigdata;

import com.bigdata.bean.WordCntResultDo;
import com.bigdata.util.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;


public class FlinkToRedisResult extends RichSinkFunction<WordCntResultDo> {

    protected static final String REDIS_RESULT_MAP_KEY = "flinkWordMap";



    public void open(Configuration parameters) throws Exception {


    }



    @Override
    public void invoke(WordCntResultDo value, Context context) throws Exception {

        System.out.println(" value:" + value);
        if (StringUtils.isNotEmpty(value.getName()))
        {
//            RedisUtil.set("flinkWordCnt:" + value.getName(),String.valueOf(value.getCnt()),0);
            RedisUtil.addHash(REDIS_RESULT_MAP_KEY,value.getName(),String.valueOf(value.getCnt()));
        }

    }

    public void close() throws Exception {

    }
}
