package edu.sjsu.cs286.emailcf.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class EmailClassifier {

    public static void main(String[] args) throws Exception {
        String myTextFile = "/user/user01/cs286-spark-nb-email-classification/dataset/subjects.csv";
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("EmailClassifier");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        /* Read Data */
        // Read data from CSV
        JavaPairRDD<String, List<String>> rdd = sc.textFile(myTextFile).mapToPair(
            new PairFunction<String, String, List<String>>() {
                public Tuple2<String, List<String>> call(String line) {
                    String[] csValues = line.toLowerCase().replaceAll("[\"]", "").replaceAll("\\s+", ",").split(",");
                    List<String> words = Arrays.asList(Arrays.copyOfRange(csValues, 1, csValues.length));
                    return new Tuple2<String, List<String>>(csValues[0], words);  
                }
            });

        // Call WordCount
		// Extract the string lines
        
		JavaRDD<String> lines = rdd.flatMap(
            new FlatMapFunction<Tuple2<String, List<String>>, String>() {

					public Iterable<String> call(Tuple2<String, List<String>> t) {
						return t._2;
					}
            });

		// Turn the words into (word, 1) pairs
		JavaPairRDD<String, Integer> wordOnePairs = lines
				.mapToPair(new PairFunction<String, String, Integer>() {

					public Tuple2<String, Integer> call(String x) {
						return new Tuple2<String, Integer>(x, 1);
					}
				});
		
		// reduce add the pairs by key to produce counts
		JavaPairRDD<String, Integer> counts = wordOnePairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					public Integer call(Integer x, Integer y) {
						return x + y;
					}
				});
                
        /* Split data as testing and training */
        // 70% is training, 30% is testing data
        double trainingRatio = 0.7;
        
        
        /* Training */
        // Create model
        

        
        /* Testing */
        // Test the classifier
        
        /* Confusion Matrix*/

        System.out.println("Precision: ");
        System.out.println("Recall: ");
        System.out.println("Accuracy: ");
        counts.saveAsTextFile("/user/user01/cs286-spark-nb-email-classification/output/counts");
    }
}
