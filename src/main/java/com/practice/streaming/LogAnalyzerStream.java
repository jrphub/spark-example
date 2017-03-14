package com.practice.streaming;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.practice.sparktest.ApacheAccessLog;

public class LogAnalyzerStream {

	//TODO updateStateByKey functionality
	// These static variables stores the running content size values.
	/*private static final AtomicLong runningCount = new AtomicLong(0);
	private static final AtomicLong runningSum = new AtomicLong(0);
	private static final AtomicLong runningMin = new AtomicLong(Long.MAX_VALUE);
	private static final AtomicLong runningMax = new AtomicLong(Long.MIN_VALUE);*/

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("logAnalyzerStream")
				.setMaster("local[2]");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaStreamingContext stream = new JavaStreamingContext(jsc,
				Durations.seconds(2));
		// JavaReceiverInputDStream<String> logDataStream = stream
		// .socketTextStream("localhost", 9999);

		JavaDStream<String> logDataStream = stream
				.textFileStream("file:///home/jrp/workspace_1/spark-example/data/stream_data/");
		//TODO
		//jsc.checkpointFile("file:///home/jrp/workspace_1/spark-example/data/checkpoint_data/");
		SQLContext sqlContext = new SQLContext(jsc);
		JavaDStream<ApacheAccessLog> accessLogStream = logDataStream
				.map(new Function<String, ApacheAccessLog>() {
					@Override
					public ApacheAccessLog call(String line) throws Exception {
						return ApacheAccessLog.parseFromLogLine(line);
					}
				});

		JavaDStream<ApacheAccessLog> goodAccessLogStream = accessLogStream
				.filter(new Function<ApacheAccessLog, Boolean>() {

					@Override
					public Boolean call(ApacheAccessLog logObj)
							throws Exception {
						return logObj != null;
					}
				});

		goodAccessLogStream.cache();

		JavaDStream<ApacheAccessLog> windowDStream = goodAccessLogStream
				.window(Durations.seconds(20), Durations.seconds(10));

		windowDStream
				.foreachRDD(new Function<JavaRDD<ApacheAccessLog>, Void>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Void call(JavaRDD<ApacheAccessLog> accessLog)
							throws Exception {
						if (accessLog.count() == 0) {
							System.out
									.println("No access logs in this time interval");
							return null;
						}

						DataFrame accessLogFrame = sqlContext.createDataFrame(
								accessLog, ApacheAccessLog.class);
						accessLogFrame.registerTempTable("logs");
						accessLogFrame.sqlContext().cacheTable("logs");
						;

						// The average, min, and max content size of responses
						// returned from the
						// server
						DataFrame stats = sqlContext
								.sql("Select sum(contentSize), count(*), min(contentSize), max(contentSize) from logs");
						stats.show();

						Row row = stats.head();
						//TODO
						/*runningCount.addAndGet(row.getAs(1));
						runningSum.addAndGet(row.getAs(0));
						runningMin.set(row.getAs(2));
						runningMax.set(row.getAs(3));*/
						
						System.out.println("In this window ...");
						System.out.println("Average : " + (long) row.getAs(0)
								/ (long) row.getAs(1));
						System.out.println("Min : " + row.getAs(2));
						System.out.println("max : " + row.getAs(3));
						
						//TODO
						/*System.out.println("\nSo far ...");
						System.out.println("Average : " + runningMax.get()/runningCount.get());
						System.out.println("Min : " + runningMin.get());
						System.out.println("Max : " + runningMax.get());*/

						// A count of response code's returned.
						DataFrame codeDataframe = sqlContext
								.sql("select count(responseCode) from logs");

						System.out.println("No. of response code : "
								+ codeDataframe.first().get(0));

						// All IPAddresses that have accessed this server more
						// than N (=10)
						// times.
						DataFrame ipAddressFrame = sqlContext
								.sql("select ipAddress, count(*) as count from logs group by ipAddress having count > 10");
						System.out
								.println("Here is the list of IPAddresses acces more than 10 times");
						List<Row> ips = ipAddressFrame.collectAsList();
						for (Row ip : ips) {
							System.out.println(ip.get(0));
						}

						// The top 10 endpoints requested by count
						DataFrame endPointFrame = sqlContext
								.sql("Select endpoint, count(*) as count from logs group by endpoint order by count desc limit 10");
						List<Row> endPoints = endPointFrame.collectAsList();
						System.out.println("Top 10 endpoints accessed ... ");
						for (Row endPoint : endPoints) {
							System.out.println(endPoint.get(0));
						}

						return null;
					}

				});

		stream.start();
		stream.awaitTermination();

	}
	
	//TODO
	/*private static Function2<List<Long>, Optional<Long>, Optional<Long>> COMPUTE_RUNNING_SUM = new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
		@Override
		public Optional<Long> call(List<Long> nums, Optional<Long> current)
				throws Exception {
			long sum = current.or(0L);
			for (long i : nums) {
				sum += i;
			}
			return Optional.of(sum);
		}
	};*/

}
