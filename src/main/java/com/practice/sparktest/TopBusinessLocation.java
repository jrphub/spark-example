package com.practice.sparktest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/*
 * Find out the location which made highest business
 *
 * i.e.
 * location | Sum (Purchase amount) : Highest only
 * 
 * Users : id, email, language, location, age, gender
 * Transactions : trans_id, prod_id, user_id, purchase_amt, item_description
 * 
 */
public class TopBusinessLocation {
    private static JavaSparkContext jsc;
    
    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("TopBusinessLocation").setMaster("local"));
        setJsc(sc);
        JavaPairRDD<String, Integer> output_rdd = run("transactions.txt", "users.txt");
        output_rdd.saveAsHadoopFile("output_TopBusinessLocation", String.class, Integer.class, TextOutputFormat.class);
        sc.close();
    }

	private static JavaPairRDD<String, Integer> run(String t, String u) {
		//read transactions.txt
		JavaRDD<String> t_rdd = getJsc().textFile(t);
		
		//select columns
		JavaPairRDD<String, Integer> t_select = t_rdd.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				String[] t_columns = s.split("\t");
				return new Tuple2<String, Integer>(t_columns[2], Integer.valueOf(t_columns[3]));
			}
		});
		
		
		//read users.txt
		JavaRDD<String> u_rdd = getJsc().textFile(u);
		
		//Select columns
		JavaPairRDD<String, String> u_select = u_rdd.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] u_columns = s.split("\t");
				return new Tuple2<String, String>(u_columns[0], u_columns[3]);
			}
		});
		
		//Join data
		//location, purchase_amt
		JavaRDD<Tuple2<String, Integer>> joinedData = u_select.join(t_select).values();
		
		//aggregate of Purchase amounts by location
		//location, [purchase_amt, purchase_amt]
		JavaPairRDD<String, Integer> aggPurchaseAmt = joinedData.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> t)
					throws Exception {
				
				return new Tuple2<String, Integer>(t._1, t._2);
			}
		});
		
		//Sum of Purchase Amt
		//location, sum(purchase_amt)
		JavaPairRDD<String, Integer> sumPurchaseAmt = aggPurchaseAmt.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		Tuple2<String, Integer> sortedByValues = sumPurchaseAmt.max(new CompareByValues());
		
		List<Tuple2<String, Integer>>  tuples = new ArrayList<Tuple2<String, Integer>>();
		tuples.add(sortedByValues);
		JavaPairRDD<String, Integer> rdd_tuple = getJsc().parallelizePairs(tuples);
		
	return rdd_tuple;
	}


	public static JavaSparkContext getJsc() {
		return jsc;
	}

	public static void setJsc(JavaSparkContext jsc) {
		TopBusinessLocation.jsc = jsc;
	}
	
	static class CompareByValues implements Comparator<Tuple2<String, Integer>>, Serializable {

		@Override
		public int compare(Tuple2<String, Integer> t1,
				Tuple2<String, Integer> t2) {
			if (t1._2 > t2._2)
				return 1;
			else
				return -1;
		}
		
	}
    
    	
}