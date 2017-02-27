package com.practice.dataframe;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DynamicPersonDF {

	public static void main(String[] args) throws IOException {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("DynamicPersonDF").setMaster("local"));
		SQLContext sqlContext = new SQLContext(sc);
		
		//Get schema or header from external file
		BufferedReader bfr = new BufferedReader(new FileReader("data/headerPeople.txt"));
		String line=null;
		StringBuilder header=new StringBuilder();
		while((line = bfr.readLine()) != null) {
			header.append(line.trim());
		}
		
		//Get separator from outside or hard code it
		String separator = ",";
		
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String field : header.toString().split(separator)) {
			fields.add(DataTypes.createStructField(field, DataTypes.StringType, true));
		}
		
		//Make schema ready
		StructType schema = DataTypes.createStructType(fields);
		
		//Now Data
		JavaRDD<String> people = sc.textFile("data/people.txt");
		
		JavaRDD<Row> rows = toRows(people, schema);
		
		// Apply the schema to the RDD.
		DataFrame peopleDataFrame = sqlContext.createDataFrame(rows, schema);
		
		// Register the DataFrame as a table.
		peopleDataFrame.registerTempTable("people");
		
		// SQL can be run over RDDs that have been registered as tables.
		DataFrame results = sqlContext.sql("SELECT name FROM people");
		
		results.show();
		
	}

	private static JavaRDD<Row> toRows(JavaRDD<String> people, StructType schema) {
		final String[] queryFields = schema.fieldNames();
		final int noOfFields = queryFields.length;
		  JavaRDD<Row> rows = people.map(new Function<String, Row>() {
		    public Row call(String rowData) throws Exception {
		      //
		      Object[] vals = new Object[noOfFields];
		      String[] rowValues = rowData.split(",");
		      for (int f = 0; f < noOfFields; f++) {
		        Object fieldValue = rowValues[f];
		        if (fieldValue != null) {
		          if (fieldValue instanceof Collection) {
		            vals[f] = ((Collection) fieldValue).toArray();
		          } else if (fieldValue instanceof Date) {
		            vals[f] = new java.sql.Timestamp(((Date) fieldValue).getTime());
		          } else {
		            vals[f] = fieldValue;
		          }
		        }
		      }
		      return RowFactory.create(vals);
		    }
		  });
		return rows;
	}

}
