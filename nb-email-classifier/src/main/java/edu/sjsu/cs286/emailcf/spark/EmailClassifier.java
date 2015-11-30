package edu.sjsu.cs286.emailcf.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sjsu.cs286.wordcount.SparkWordCount;

public class EmailClassifier {

	public static void main(String[] args) throws Exception {
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("EmailClassifier");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/* Read Data */
		// Read data from CSV
		JavaRDD<String> distFile = sc.textFile("/user/user01/cs286-spark-nb-email-classification/dataset/subjects.csv");

		/* Split data as testing and training */
		// 70% is training, 30% is testing data
		double trainingRatio = 0.7;
		/* Training */
		// Create model
		
		// Call WordCount
		
		
		/* Testing */
		// Test the classifier
		
		/* Confusion Matrix*/

        	System.out.println("Precision: ");
        	System.out.println("Recall: ");
        	System.out.println("Accuracy: ");
		
	}
}
