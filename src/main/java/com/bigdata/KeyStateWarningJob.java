/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bigdata;

import com.bigdata.bean.WordCntResultDo;
import com.bigdata.status.ThresSholdWarning;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class KeyStateWarningJob {

    protected static  List<String> wordList = Arrays.asList("Spark","Hadoop","Storm");

    private static final String KAFKA_TOPIC = "flinkKafka";
    private static final String KAFKA_RESULT_TOPIC = "flink_result_kafka";
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put("bootstrap.servers","47.112.111.193:9092");

        DataStreamSource ds = env.addSource(new FlinkKafkaConsumer(KAFKA_TOPIC,new SimpleStringSchema(),properties));
        SingleOutputStreamOperator fms = ds.flatMap(new FlatMapFunction<String,String>() {
            @Override
            public void flatMap(String o, Collector collector) throws Exception {
//                System.out.println(" loop one line o:" + o);
                String[] strArray = o.split(" ");
                for (String s : strArray) {
                    collector.collect(s);
                }
            }
        });


        SingleOutputStreamOperator ms = fms.map(new MapFunction<String,Tuple2<String,Long>>() {
            @Override
            public Tuple2<String,Long> map(String o) throws Exception {
                return Tuple2.of(o,1l);
            }
        });
//        ms.print();

        WindowedStream ws = ms.keyBy(0).timeWindow(Time.seconds(30));
        SingleOutputStreamOperator sums =ws.reduce(new ReduceFunction<Tuple2<String,Long>>() {
            @Override
            public Tuple2<String,Long> reduce(Tuple2<String,Long> o, Tuple2<String,Long> t1) throws Exception {
                return new Tuple2<String,Long>(o.f0,(o.f1+t1.f1));
            }
        });
        KeyedStream keyedStream = sums.keyBy(0);
//        keyedStream.print();
        keyedStream.flatMap(new ThresSholdWarning(10l,2)).returns(Types.TUPLE(Types.STRING,Types.LIST(Types.LONG))).print();



//        SingleOutputStreamOperator r2 = sums.map(new MapFunction<Tuple2<String,Integer>,String>() {
//            @Override
//            public String map(Tuple2<String,Integer> o) throws Exception {
//                StringBuilder sb = new StringBuilder();
//                sb.append(" key:" + o.f0);
//                sb.append(" cnt:" + o.f1);
//                return sb.toString();
//            }
//        });
//
//        r2.print();



//        KafkaSerializationSchema<Tuple2<String,Integer>> kafkaSerializationSchema = new KafkaSerializationSchema<Tuple2<String,Integer>>() {
//
//            @Override
//            public ProducerRecord<byte[], byte[]> serialize(Tuple2<String,Integer> o, @Nullable Long aLong) {
//                StringBuilder sb = new StringBuilder();
//                sb.append(" name:" + o.f0);
//                sb.append(" value:" + o.f1);
//                return new ProducerRecord<byte[], byte[]>(KAFKA_RESULT_TOPIC,o.f0.getBytes(),sb.toString().getBytes());
//            }
//
//        };
//
//        FlinkKafkaProducer<byte[]> flinkKafkaProducer = new FlinkKafkaProducer(KAFKA_RESULT_TOPIC,kafkaSerializationSchema,properties,
//               FlinkKafkaProducer.Semantic.EXACTLY_ONCE );
//
//        sums.addSink(flinkKafkaProducer);


//        SingleOutputStreamOperator sums =  ws.sum(1);
//        sums.print();



		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
