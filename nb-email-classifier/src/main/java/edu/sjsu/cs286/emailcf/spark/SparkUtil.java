package edu.sjsu.cs286.emailcf.spark;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import edu.sjsu.cs286.emailcf.java.WordFrequency;

/**
 * 
 * Spark Utility
 */
public class SparkUtil {

	/*
	 * Takes an input and outputs count of each word
	 */
	public static JavaPairRDD<String, Integer> countWords(
			JavaRDD<String> input) throws Exception {

		/* Map */
		// Split each line to multiple words
		JavaRDD<String> words = input.flatMap(new SplitWordsFunction());

		// Turn the words into (word, 1) pairs
		JavaPairRDD<String, Integer> wordOnePairs = words.mapToPair(new WordTupleFunction());

		/* Reduce */
		// Add the pairs by key to produce counts
		JavaPairRDD<String, Integer> counts = wordOnePairs.reduceByKey(new AddCountsFunction());

		return counts;
	}
	
	/**
	 * 
	 * @param countRDD (RDD with wordcount)
	 * @return Integer
	 */
	public static Integer addCounts(JavaPairRDD<String, Integer> countRDD){
		return countRDD.values().reduce(new AddCountsFunction());
	}

}

/**
 * Takes a String as input, returns an array of Strings
 */
@SuppressWarnings("serial")
class SplitWordsFunction implements FlatMapFunction<String, String> {
	
	@Override
	public Iterable<String> call(String x) {
		return Arrays.asList(x.split(" "));
	}
}

/**
 * Takes a String as input, returns a Tuple
 */
@SuppressWarnings("serial")
class WordTupleFunction implements PairFunction<String, String, Integer> {

	@Override
	public Tuple2<String, Integer> call(String x) {
		return new Tuple2<String, Integer>(x, 1);
	}
}

/**
 * Takes 2 integers, returns the sum
 */
@SuppressWarnings("serial")
class AddCountsFunction implements Function2<Integer, Integer, Integer> {

	@Override
	public Integer call(Integer x, Integer y) {
		return x + y;
	}
}

/**
 * Creates WordFrequency and sets spam count
 */
@SuppressWarnings("serial")
class UpdateSpamFrequencyFunction implements Function<Integer, WordFrequency> {
	@Override
	public WordFrequency call(Integer s) {
		WordFrequency wordFreq = new WordFrequency();
		wordFreq.setCntSpam(s);
		return wordFreq;
	}
}

/**
 * Creates WordFrequency and sets ham count
 */
@SuppressWarnings("serial")
class UpdateHamFrequencyFunction implements Function<Integer, WordFrequency> {
	@Override
	public WordFrequency call(Integer s) {
		WordFrequency wordFreq = new WordFrequency();
		wordFreq.setCntHam(s);
		return wordFreq;
	}
}