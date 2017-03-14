package com.practice.dataframe;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.practice.sparktest.ApacheAccessLog;

public class LogAnalyzerDataframe {

	public static void main(String[] args) {
		SparkConf sconf = new SparkConf().setAppName("logAnalyzerDataframe")
				.setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sconf);

		SQLContext sqlContext = new SQLContext(jsc);

		JavaRDD<ApacheAccessLog> accesslogRDD = jsc.textFile(
				"data/access_log.txt").map(
				new Function<String, ApacheAccessLog>() {
					@Override
					public ApacheAccessLog call(String line) throws Exception {
						return ApacheAccessLog.parseFromLogLine(line);
					}

				});

		JavaRDD<ApacheAccessLog> goodAccessLogRDD = accesslogRDD
				.filter(new Function<ApacheAccessLog, Boolean>() {
					@Override
					public Boolean call(ApacheAccessLog accessLogObj)
							throws Exception {
						return accessLogObj != null;
					}
				});

		DataFrame df = sqlContext.createDataFrame(goodAccessLogRDD,
				ApacheAccessLog.class);

		df.registerTempTable("logs");
		df.sqlContext().cacheTable("logs");

		// The average, min, and max content size of responses returned from the
		// server
		DataFrame stats = sqlContext
				.sql("Select sum(contentSize), count(*), min(contentSize), max(contentSize) from logs");
		stats.show();

		Row row = stats.head();

		System.out.println("Average : " + (long) row.getAs(0)
				/ (long) row.getAs(1));
		System.out.println("Min : " + row.getAs(2));
		System.out.println("max : " + row.getAs(3));

		// A count of response code's returned.
		DataFrame codeDataframe = sqlContext
				.sql("select count(responseCode) from logs");

		System.out.println("No. of response code : "
				+ codeDataframe.first().get(0));

		// All IPAddresses that have accessed this server more than N (=10)
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

		jsc.close();

	}
}
