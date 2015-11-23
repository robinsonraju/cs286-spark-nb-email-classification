package edu.sjsu.cs286.wordcount;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * Spark WordCount 
 */
public class SparkWordCount {

	/*
	 * Takes an input and outputs count of each word
	 */
	public static JavaPairRDD<String, Integer> countWords(JavaRDD<String> input, JavaSparkContext sc) throws Exception {
		
		// map/split each line to multiple words
		JavaRDD<String> words = input
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String x) {
						return Arrays.asList(x.split(" "));
					}
				});

		// Turn the words into (word, 1) pairs
		JavaPairRDD<String, Integer> wordOnePairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String x) {
						return new Tuple2(x, 1);
					}
				});
		
		// reduce add the pairs by key to produce counts
		JavaPairRDD<String, Integer> counts = wordOnePairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer x, Integer y) {
						return x + y;
					}
				});

		return counts;
	}
}
