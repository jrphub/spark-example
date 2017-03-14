package com.practice.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class NetworkWordCount {
	
	/*nc -lk 9999*/

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
		
		JavaReceiverInputDStream jrids = jsc.socketTextStream("localhost", 9999);
		
		@SuppressWarnings("unchecked")
		JavaDStream<String> words = jrids.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			};
		});
		
		JavaPairDStream<String, Integer> wordPair = words.mapToPair(new PairFunction<String, String, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairDStream<String, Integer> wordGroup = wordPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer c0, Integer c1) throws Exception {
				return c0 + c1;
			}
		});
		
		wordGroup.print();
		
		jsc.start();
		jsc.awaitTermination();
		
		
		
	}

}
