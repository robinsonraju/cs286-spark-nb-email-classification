package edu.sjsu.cs286.emailcf.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.PairFunction;

import edu.sjsu.cs286.wordcount.SparkWordCount;

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
                    String[] csValues = line.toLowerCase().replaceAll("[\"]", "").split(",");
                    List<String> words = Arrays.asList(Arrays.copyOfRange(csValues, 1, csValues.length));
                    return new Tuple2<String, List<String>>(csValues[0], words);  
                }
            });

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
