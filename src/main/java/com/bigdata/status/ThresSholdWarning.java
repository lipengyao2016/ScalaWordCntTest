package com.bigdata.status;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ThresSholdWarning extends RichFlatMapFunction<Tuple2<String,Long>,Tuple2<String,List<Long>>> {

    private Long thresShold;
    private Integer warningNumCnt;
    private transient ListState<Long> warningDataList;

    public ThresSholdWarning(Long thresShold, Integer warningNumCnt) {
        this.thresShold = thresShold;
        this.warningNumCnt = warningNumCnt;
    }

    public void open(Configuration parameters) throws Exception {

        warningDataList = getRuntimeContext().getListState(new ListStateDescriptor<Long>("warning",Long.class));
    }

    @Override
    public void flatMap(Tuple2<String, Long> stringLongTuple2, Collector<Tuple2<String, List<Long>>> collector) throws Exception {
        if (stringLongTuple2.f1 > thresShold)
        {
            warningDataList.add(stringLongTuple2.f1);
            System.out.println(" f0:" + stringLongTuple2.f0 + " f1:" + stringLongTuple2.f1
                    + " exceed thresShold" );
        }
        List<Long> warningList = Lists.newArrayList(warningDataList.get().iterator());

        System.out.println(" f0:" + stringLongTuple2.f0 + " f1:" + stringLongTuple2.f1
        + " exceed size:" + warningList.size());

        if (warningList.size() > warningNumCnt)
        {
            System.out.println(" will warning exceed thresShold warningNumCnt f0:" + stringLongTuple2.f0 + " f1:" + stringLongTuple2.f1
                    + " exceed size:" + warningList.size());

            collector.collect(new Tuple2<>(stringLongTuple2.f0 + " 超过指定阀值:" + thresShold + "," + warningNumCnt + "次",warningList));
            warningDataList.clear();
        }

    }
}
