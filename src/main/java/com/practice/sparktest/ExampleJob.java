package com.practice.sparktest;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

import com.google.common.base.Optional;

/*
 * No. of unique locations in which each product is sold
 *
 * i.e.
 * prod_id | Count of Distinct locations
 * 
 * Users : id, email, language, location, age, gender
 * Transactions : trans_id, prod_id, user_id, purchase_amt, item_description
 * 
 */
public class ExampleJob {
    private static JavaSparkContext sc;
    
    public ExampleJob(JavaSparkContext sc){
    	this.sc = sc;
    }
    
    public static void main(String[] args) throws Exception {
    	//for local mode, no hadoop to be started
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));
        ExampleJob job = new ExampleJob(sc);
        //for debugging purpose, creating intermediate files in debug directory
        File debugDir = new File("debug");
        deleteDirIfExists(debugDir);
        
        JavaPairRDD<String, String> output_rdd = job.run("data/transactions.txt", "data/users.txt");
        File outputDir = new File("output");
        if (outputDir.exists()) {
        	System.out.println("Deleting output directory : " + outputDir + " ...");
        	File[] files = outputDir.listFiles();
        	for (File file : files) {
        		file.delete();
        	}
        	outputDir.delete();
        }
        output_rdd.saveAsHadoopFile("output", String.class, String.class, TextOutputFormat.class);
        sc.close();
    }
    
    private static void deleteDirIfExists(File debugDir) {
		if (debugDir.exists()) {
			File[] allFiles = debugDir.listFiles();
			for (File file : allFiles) {
				if (file.isDirectory()) {
					deleteDirIfExists(file);
				} else {
					file.delete();
				}
			}
			debugDir.delete();
		}
		
	}

	public static JavaPairRDD<String, String> run(String t, String u){
        JavaRDD<String> transactionInputFile = sc.textFile(t);
        
        //(user_id, prod_id) : mapToPair
        JavaPairRDD<Integer, Integer> transactionPairs = transactionInputFile.mapToPair(new PairFunction<String, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(String s) {
                String[] transactionSplit = s.split("\t");
                return new Tuple2<Integer, Integer>(Integer.valueOf(transactionSplit[2]), Integer.valueOf(transactionSplit[1]));
            }
        });
        
        transactionPairs.saveAsTextFile("debug/01_transactionPairs");
        
        
        
        JavaRDD<String> customerInputFile = sc.textFile(u);
        //(id, location) : mapToPair
        JavaPairRDD<Integer, String> customerPairs = customerInputFile.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) {
                String[] customerSplit = s.split("\t");
                return new Tuple2<Integer, String>(Integer.valueOf(customerSplit[0]), customerSplit[3]);
            }
        });
        
        customerInputFile.saveAsTextFile("debug/02_customerInputFile");

        Map<Integer, Object> result = countData(modifyData(joinData(transactionPairs, customerPairs)));
        
        //To make JavaPairRDD out of result, convert map to list
        List<Tuple2<String, String>> output = new ArrayList<>();
	    for (Entry<Integer, Object> entry : result.entrySet()){
	    	output.add(new Tuple2<>(entry.getKey().toString(), String.valueOf((long)entry.getValue())));
	    }
	    
	    //Pass the list to parallelizePairs to make JavaPairRDD
	    JavaPairRDD<String, String> output_rdd = sc.parallelizePairs(output);
	    
	    output_rdd.saveAsTextFile("debug/05_result");
	    return output_rdd;
	}
    
    //prod_id, location : distinct values
	public static JavaRDD<Tuple2<Integer,Optional<String>>> joinData(JavaPairRDD<Integer, Integer> t, JavaPairRDD<Integer, String> u){
        JavaRDD<Tuple2<Integer,Optional<String>>> leftJoinOutput = t.leftOuterJoin(u).values().distinct();
        leftJoinOutput.saveAsTextFile("debug/03_leftJoinOutput");
        return leftJoinOutput;
	}
	
	//prod_id, location : non-null values because of using Optional Class which box values and avoid null values
	public static JavaPairRDD<Integer, String> modifyData(JavaRDD<Tuple2<Integer,Optional<String>>> d){
		JavaPairRDD<Integer, String> modify = d.mapToPair(KEY_VALUE_PAIRER);
		modify.saveAsTextFile("debug/04_modifyData");
		return modify;
	}
	
	//Map<prod_id, Count of distinct location>
	public static Map<Integer, Object> countData(JavaPairRDD<Integer, String> d){
        Map<Integer, Object> result = d.countByKey();
        return result;
	}
	
	//To box values and avoid null values
	public static final PairFunction<Tuple2<Integer, Optional<String>>, Integer, String> KEY_VALUE_PAIRER =
	    new PairFunction<Tuple2<Integer, Optional<String>>, Integer, String>() {
	    	public Tuple2<Integer, String> call(
	    			Tuple2<Integer, Optional<String>> a) throws Exception {
				// a._2.isPresent()
	    		return new Tuple2<Integer, String>(a._1, a._2.get());
	    	}
		};
	
}