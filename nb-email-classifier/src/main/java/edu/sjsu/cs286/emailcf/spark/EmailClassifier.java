package edu.sjsu.cs286.emailcf.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sjsu.cs286.wordcount.SparkWordCount;

public class EmailClassifier {

	public static void main(String[] args) throws Exception {
		String inputFileSpam = args[0];
		String inputFileHam = args[1];
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("EmailClassifier");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/* Read Data */
		// Read data from CSV
		JavaRDD<String> inputSpam = sc.textFile(inputFileSpam);
		JavaRDD<String> inputHam = sc.textFile(inputFileHam);

		/* Split data as testing and training */
		// 70% is training, 30% is testing data
		
		/* Training */
		// Create model
		
		// Call WordCount
		JavaPairRDD<String, Integer> spamCounts = SparkWordCount.countWords(inputSpam, sc);
		JavaPairRDD<String, Integer> hamCounts = SparkWordCount.countWords(inputHam, sc);
		
		
		/* Testing */
		// Test the classifier
		
		/* Confusion Matrix*/
		
	}
}
