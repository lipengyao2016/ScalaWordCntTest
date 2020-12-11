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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

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
public class LocalStreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String filePath = "E:\\data\\bigdata\\exec-log";
        DataStreamSource ds = env.readFile(new TextInputFormat(new Path(filePath)),filePath, FileProcessingMode.PROCESS_ONCE,1);
        SingleOutputStreamOperator fms = ds.flatMap(new FlatMapFunction<String,String>() {
            @Override
            public void flatMap(String o, Collector collector) throws Exception {
                System.out.println(" loop one line o:" + o);
                String[] strArray = o.split(" ");
                for (String s : strArray) {
                    collector.collect(s);
                }
            }
        });


//        SingleOutputStreamOperator fms = ds.flatMap(
//           ( o, collector) -> {
//                System.out.println(" loop one line o:" + o);
//                String[] strArray = ((String)o).split(" ");
//                for (String s : strArray) {
//                    collector.collect(s);
//                }
//            }
//        ).returns(Types.STRING);


//        fms.print();


        SingleOutputStreamOperator ms = fms.map(new MapFunction<String,Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> map(String o) throws Exception {
                return Tuple2.of(o,1);
            }
        });
//        ms.print();


        SingleOutputStreamOperator sums =  ms.keyBy(0)/*.timeWindow(Time.milliseconds(1))*/.sum(1);
        sums.print();

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
