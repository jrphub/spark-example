package com.matthewrathbone.sparktest;

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

import scala.Tuple2;

import com.google.common.base.Optional;

public class ExampleJob {
    private static JavaSparkContext sc;
    
    public ExampleJob(JavaSparkContext sc){
    	this.sc = sc;
    }
    
    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));
        ExampleJob job = new ExampleJob(sc);
        JavaPairRDD<String, String> output_rdd = job.run("transactions.txt", "users.txt");
        output_rdd.saveAsHadoopFile("output", String.class, String.class, TextOutputFormat.class);
        sc.close();
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
        
        JavaRDD<String> customerInputFile = sc.textFile(u);
        //(id, location) : mapToPair
        JavaPairRDD<Integer, String> customerPairs = customerInputFile.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) {
                String[] customerSplit = s.split("\t");
                return new Tuple2<Integer, String>(Integer.valueOf(customerSplit[0]), customerSplit[3]);
            }
        });

        Map<Integer, Object> result = countData(modifyData(joinData(transactionPairs, customerPairs)));
        
        //To make JavaPairRDD out of result, convert map to list
        List<Tuple2<String, String>> output = new ArrayList<>();
	    for (Entry<Integer, Object> entry : result.entrySet()){
	    	output.add(new Tuple2<>(entry.getKey().toString(), String.valueOf((long)entry.getValue())));
	    }
	    
	    //Pass the list to parallelizePairs to make JavaPairRDD
	    JavaPairRDD<String, String> output_rdd = sc.parallelizePairs(output);
	    return output_rdd;
	}
    
    //prod_id, location : distinct values
	public static JavaRDD<Tuple2<Integer,Optional<String>>> joinData(JavaPairRDD<Integer, Integer> t, JavaPairRDD<Integer, String> u){
        JavaRDD<Tuple2<Integer,Optional<String>>> leftJoinOutput = t.leftOuterJoin(u).values().distinct();
        return leftJoinOutput;
	}
	
	//prod_id, location : non-null values because of using Optional Class which box values and avoid null values
	public static JavaPairRDD<Integer, String> modifyData(JavaRDD<Tuple2<Integer,Optional<String>>> d){
		return d.mapToPair(KEY_VALUE_PAIRER);
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