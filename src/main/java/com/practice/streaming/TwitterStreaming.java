package com.practice.streaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

public class TwitterStreaming {

	public static void main(String[] args) {
		String consumerKey = "fGvLDuIPAvJgL3c9bjbfyicpA";
		String consumerSecret = "LwvOeBOxFIqYfXZD7AKMsouagDczwEynpNjj1P4qsCkI9WcRXi";
		String accessToken = "<accessToken>";
		String accessTokenSecret = "<Secrettoken>";
		
		String[] filters = new String[] { "narendramodi", "amitabh" };

		// Set the system properties so that Twitter4j library used by Twitter
		// stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret",
				accessTokenSecret);

		SparkConf sparkConf = new SparkConf().setAppName("TwitterStreaming");

		// check Spark configuration for master URL, set it to local if not
		// configured
		if (!sparkConf.contains("spark.master")) {
			sparkConf.setMaster("local[2]");
		}

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				new Duration(2000));
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(
				jssc, filters);
		
		JavaDStream<String> words = stream
				.flatMap(new FlatMapFunction<Status, String>() {
					@Override
					public List<String> call(Status s) {
						return Arrays.asList(s.getText().split(" "));
					}
				});
		
		JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
		      @Override
		      public Boolean call(String word) {
		        return word.startsWith("#");
		      }
		    });
		
		hashTags.print();
		
		jssc.start();
		jssc.awaitTermination();
	    

	}

}
