package com.practice.sparktest;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/*
 * Sum of Purchase amount by each gender
 *
 * i.e.
 * Gender | Sum (Purchase amount)
 * 
 * Users : id, email, language, location, age, gender
 * Transactions : trans_id, prod_id, user_id, purchase_amt, item_description
 * 
 */
public class PurchaseByGender {
    private static JavaSparkContext jsc;
    
    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("PurchaseByGender").setMaster("local"));
        setJsc(sc);
        JavaPairRDD<String, Integer> output_rdd = run("data/transactions.txt", "data/users.txt");
        output_rdd.saveAsHadoopFile("output_PurchaseByGender", String.class, Integer.class, TextOutputFormat.class);
        sc.close();
    }

	private static JavaPairRDD<String, Integer> run(String t, String u) {
		//read transactions.txt
		JavaRDD<String> t_rdd = getJsc().textFile(t);
		
		//select require fields only
		//user_id, purchase_amt
		JavaPairRDD<String, Integer> trans_select = t_rdd.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				String[] trans_split = s.split("\t");
				return new Tuple2<String, Integer>(trans_split[2], Integer.valueOf(trans_split[3]));
			}
		});
		
		//read users.txt
		JavaRDD<String> u_rdd = getJsc().textFile(u);
		
		//Select require fields only
		//id, gender
		JavaPairRDD<String, String> user_select = u_rdd.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] users_split = s.split("\t");
				return new Tuple2<String, String>(users_split[0], users_split[5]);
			}
		});
		
		//Inner join
		//gender, purchase_amt
		JavaRDD<Tuple2<String, Integer>> joinedData = user_select.join(trans_select).values();
		
		//Sum of purchase amount by gender
		JavaPairRDD<String, Integer> aggByGender = joinedData.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> s)
					throws Exception {
				return new Tuple2<String, Integer>(s._1, s._2);
			}
		
		});
		
		JavaPairRDD<String, Integer> sumByGender = aggByGender.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		return sumByGender;
	}


	public static JavaSparkContext getJsc() {
		return jsc;
	}

	public static void setJsc(JavaSparkContext jsc) {
		PurchaseByGender.jsc = jsc;
	}
    
    	
}