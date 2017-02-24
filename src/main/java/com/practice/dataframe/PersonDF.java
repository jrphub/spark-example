package com.practice.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;



public class PersonDF {
	
	
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("PersonDF").setMaster("local"));
		
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaRDD<Person> personRDD = sc.textFile("data/people.txt").map(
				new Function<String, Person>() {
					public Person call(String line) throws Exception {
						
						String[] parts = line.split(",");
						Person person = new Person();
						person.setName(parts[0]);
						person.setAge(Integer.parseInt(parts[1].trim()));
						return person;
						
						
					}
					
				});
		
		// Apply a schema to an RDD of JavaBeans and register it as a table.
		DataFrame schemaPeople = sqlContext.createDataFrame(personRDD, Person.class);
		schemaPeople.registerTempTable("people");
		
		// SQL can be run over RDDs that have been registered as tables.
		DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 22 AND age <= 25");
		
		
		// The results of SQL queries are DataFrames and support all the normal RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		/*List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
		  public String call(Row row) {
		    return "Name: " + row.getString(0);
		  }
		}).collect();
		
		for (String teenager :  teenagerNames) {
			System.out.println(teenager);
		}*/
		
		//teenagers.javaRDD().saveAsTextFile("output_person");
		//teenagers.write().save("output_dataframe_person");//stores in parquet format
		//teenagers.write().format("json").save("output_dataframe_json");
		
		DataFrame df = sqlContext.sql("SELECT * FROM text.`data/people.txt`");
		df.write().format("json").save("output_dataframe_file");
		
	}
	
		
}

