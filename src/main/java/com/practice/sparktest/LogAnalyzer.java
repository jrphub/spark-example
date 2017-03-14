package com.practice.sparktest;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class LogAnalyzer {

	/*
	 * The following statistics will be computed:
	 * 
	 * The average, min, and max content size of responses returned from the
	 * server.
	 * 
	 * A count of response code's returned.
	 * 
	 * All IPAddresses that have accessed this server more than N (=10) times.
	 * 
	 * The top endpoints requested by count.
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("log Analyzer").setMaster(
				"local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> logRDD = jsc.textFile("data/access_log.txt");

		JavaRDD<ApacheAccessLog> accessLogRDD = logRDD
				.map(new Function<String, ApacheAccessLog>() {
					@Override
					public ApacheAccessLog call(String line) throws Exception {
						return ApacheAccessLog.parseFromLogLine(line);
					}
				});
		// Filtering bad data
		JavaRDD<ApacheAccessLog> accessGoodLogRDD = accessLogRDD
				.filter(new Function<ApacheAccessLog, Boolean>() {

					@Override
					public Boolean call(ApacheAccessLog logObj)
							throws Exception {
						return logObj != null;
					}
				});
		// because multiple transformations will act on this RDD
		accessGoodLogRDD.cache();

		// To calculate average, maximum, minimum content size
		// average = total/count

		// let's have JavaRDD of Content size

		JavaRDD<Long> contentSizeRDD = accessGoodLogRDD
				.map(new Function<ApacheAccessLog, Long>() {
					@Override
					public Long call(ApacheAccessLog logLineObj)
							throws Exception {
						return logLineObj.getContentSize();

					}
				});

		contentSizeRDD.cache();

		// Avg = Total Size/Count of size

		Long totalContentSize = contentSizeRDD
				.reduce(new Function2<Long, Long, Long>() {

					@Override
					public Long call(Long size1, Long size2) throws Exception {
						return size1 + size2;
					}
				});

		Long avg = totalContentSize / contentSizeRDD.count();

		System.out.println("Average : " + avg);

		// Maximum
		System.out.println(contentSizeRDD.max(Comparator.naturalOrder()));

		// Minimum
		System.out.println(contentSizeRDD.min(LONG_NATURAL_ORDER_COMPARATOR));

		// Count of Response codes
		JavaPairRDD<Integer, Integer> responseMap = accessGoodLogRDD
				.mapToPair(new PairFunction<ApacheAccessLog, Integer, Integer>() {
					@Override
					public Tuple2<Integer, Integer> call(
							ApacheAccessLog logObject) throws Exception {
						return new Tuple2<Integer, Integer>(logObject
								.getResponseCode(), 1);
					}

				});

		JavaPairRDD<Integer, Integer> responseGroup = responseMap
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer codeCount1, Integer codeCount2)
							throws Exception {
						return codeCount1 + codeCount2;
					}
				});

		List<Tuple2<Integer, Integer>> responseList = responseGroup.take(100);

		for (Tuple2<Integer, Integer> code : responseList) {
			System.out.println(code._1 + " : " + code._2);
		}

		// To compute any IPAddress that has accessed this server more than 10
		// times
		JavaPairRDD<String, Long> ipMap = accessGoodLogRDD
				.mapToPair(new PairFunction<ApacheAccessLog, String, Long>() {
					@Override
					public Tuple2<String, Long> call(ApacheAccessLog logobj)
							throws Exception {
						return new Tuple2<String, Long>(logobj.getIpAddress(),
								1L);
					}
				});

		JavaPairRDD<String, Long> ipGroup = ipMap
				.reduceByKey(new Function2<Long, Long, Long>() {

					@Override
					public Long call(Long ipCount1, Long ipCount2)
							throws Exception {
						return ipCount1 + ipCount2;
					}
				});

		JavaPairRDD<String, Long> ipMoreThan10 = ipGroup
				.filter(new Function<Tuple2<String, Long>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, Long> ipTuple)
							throws Exception {
						return ipTuple._2 > 10L;
					}
				});

		List<Tuple2<String, Long>> ipList = ipMoreThan10.take(100);

		for (Tuple2<String, Long> ipTuple : ipList) {
			System.out.println(ipTuple._1 + " : " + ipTuple._2);
		}

		// To calculate the top endpoints requested in this log file

		JavaPairRDD<String, Long> endPointsMap = accessGoodLogRDD
				.mapToPair(new PairFunction<ApacheAccessLog, String, Long>() {
					@Override
					public Tuple2<String, Long> call(ApacheAccessLog logObj)
							throws Exception {
						return new Tuple2<String, Long>(logObj.getEndpoint(),
								1L);
					}

				});

		JavaPairRDD<String, Long> endPointGroup = endPointsMap
				.reduceByKey(new Function2<Long, Long, Long>() {

					@Override
					public Long call(Long count1, Long count2) throws Exception {
						return count1 + count2;
					}
				});

		List<Tuple2<String, Long>> endPointsList = endPointGroup.top(10,
				new ValueComparator<>(Comparator.<Long> naturalOrder()));

		for (Tuple2<String, Long> endPointTuple : endPointsList) {
			System.out.println(endPointTuple._1 + " : " + endPointTuple._2);
		}

	}

	public static class LongComparator implements Comparator<Long>,
			Serializable {

		@Override
		public int compare(Long a, Long b) {
			if (a > b)
				return 1;
			if (a.equals(b))
				return 0;

			return -1;
		}
	}

	public static Comparator<Long> LONG_NATURAL_ORDER_COMPARATOR = new LongComparator();

	private static class ValueComparator<K, V> implements
			Comparator<Tuple2<K, V>>, Serializable {
		private Comparator<V> comparator;

		public ValueComparator(Comparator<V> comparator) {
			this.comparator = comparator;
		}

		@Override
		public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
			return comparator.compare(o1._2(), o2._2());
		}
	}

}
