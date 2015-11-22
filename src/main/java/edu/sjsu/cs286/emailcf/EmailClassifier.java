package edu.sjsu.cs286.emailcf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class EmailClassifier {

	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		String outputFile = args[1];
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("EmailClassifier");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/* Read Data */
		// Read data from CSV
		JavaRDD<String> input = sc.textFile(inputFile);

		/* Split data as testing and training */
		// 70% is training, 30% is testing data
		
		/* Training */
		// Create model
		
		/* Testing */
		// Test the classifier
		
		/* Confusion Matrix*/
		
	}
}
