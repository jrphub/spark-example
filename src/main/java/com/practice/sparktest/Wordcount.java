package com.practice.sparktest;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  Here the goal is to count how much each word appears in a file and make some operation on the result.
 *  We use the mapreduce pattern to do this:
 *
 *  step 1, the mapper:
 *  - we attribute 1 to each word. And we obtain then couples (word, 1), where word is the key.
 *
 *  step 2, the reducer:
 *  - for each key (=word), the values are added and we will obtain the total amount.
 *
 *  Use the Ex0WordcountTest to implement the code.
 *
 */
public class Wordcount {

  private static String pathToFile = "data/wordcount.txt";
  private static JavaSparkContext jsc;

	public static JavaSparkContext getJsc() {
		return jsc;
	}

	public static void setJsc(JavaSparkContext jsc) {
		Wordcount.jsc = jsc;
	}

/**
   *  Load the data from the text file and return an RDD of words
   */
  public static JavaRDD<String> loadData() {
    JavaSparkContext sc = getJsc();

    // load data and create an RDD where each element will be a word
    // Here the flatMap method is used to separate the word in each line using the space separator
    // In this way it returns an RDD where each "element" is a word
    JavaRDD<String> words = sc.textFile(pathToFile).flatMap(new FlatMapFunction<String, String>() {

		@Override
		public Iterable<String> call(String line) throws Exception {
			return Arrays.asList(line.split(" "));
		}
	
    });

    return words;

  }

  /**
   *  Now count how much each word appears!
   */
  public static JavaPairRDD<String, Integer> wordcount() {
    JavaRDD<String> words = loadData();

    // Step 1: the mapper step
    // The philosophy: we want to attribute the number 1 to each word: so we create couples (word, 1) using the Tuple2 class.
    // Hint: look at the mapper methods provided by Spark
    JavaPairRDD<String, Integer> couples = words.mapToPair(new PairFunction<String, String, Integer>() {

		@Override
		public Tuple2<String, Integer> call(String word) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<String, Integer>(word, 1);
		}
	});
    
    
    // Step 2: reducer step
    // The philosophy: now you have a couple (key, value) where the key is a word, you want to aggregate the value for each word.
    // So you will use a reducer function.
    // Hint: the Spark API provides some reduce methods
    // TODO write code here
    JavaPairRDD<String, Integer> result = couples.reduceByKey(new Function2<Integer, Integer, Integer>() {
		
		@Override
		public Integer call(Integer a, Integer b) throws Exception {
			// TODO Auto-generated method stub
			return a+b;
		}
	});

    return result;
  }

  /**
   *  Now keep the word which appear strictly more than 4 times!
   */
  public static JavaPairRDD<String, Integer> filterOnWordcount() {
    JavaPairRDD<String, Integer> wordcounts = wordcount();

    // Hint: the Spark API provides a filter method
    JavaPairRDD<String, Integer> filtered = wordcounts.filter(new Function<Tuple2<String,Integer>, Boolean>() {
		
		@Override
		public Boolean call(Tuple2<String, Integer> wordCouple) throws Exception {
			// TODO Auto-generated method stub
			return (wordCouple._2 > 4);
		}
	});

    return filtered;

  }
  
  public static void main(String[] args) {
	// create spark configuration and spark context: the Spark context is the entry point in Spark.
	    // It represents the connexion to Spark and it is the place where you can configure the common properties
	    // like the app name, the master url, memories allocation...
	    SparkConf conf = new SparkConf()
	        .setAppName("Wordcount")
	        .set("spark.driver.allowMultipleContexts", "true")
	        .setMaster("local[*]"); // here local mode. And * means you will use as much as you have cores.
	  
	    setJsc(new JavaSparkContext(conf));
	  JavaPairRDD<String, Integer> result = filterOnWordcount();
	  result.saveAsHadoopFile("output_wordcount", String.class, Integer.class, TextOutputFormat.class);
	  
  }

}
