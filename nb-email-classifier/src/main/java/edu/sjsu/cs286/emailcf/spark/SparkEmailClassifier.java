package edu.sjsu.cs286.emailcf.spark;

import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import com.google.common.base.Optional;

import edu.sjsu.cs286.emailcf.java.WordFrequency;

public class SparkEmailClassifier {

	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("EmailClassifier");
		JavaSparkContext sc = new JavaSparkContext(conf);

		/* Read Data */
		// Read data from CSV
		JavaRDD<String> inputData = sc.textFile(inputFile);

		// Clean up data
		JavaRDD<String> cleanedInputData = inputData.map(new CleanUpDataFunction());
		
		/* Split data as testing and training */
		// 70% is training, 30% is testing data
		JavaRDD<String>[] tmp = cleanedInputData.randomSplit(new double[] { 0.7, 0.3 });
		JavaRDD<String> training = tmp[0]; // training set
		JavaRDD<String> test = tmp[1]; // test set
		training.cache();
		test.cache();
		
		// Filter Spam and Ham records from the input
		JavaRDD<String> spamRecs = training.filter(new SpamFilterFunction());
		JavaRDD<String> hamRecs = training.filter(new HamFilterFunction());
		JavaRDD<String> inputSpam = spamRecs.map(new SpamLabelRemoveFunction());
		JavaRDD<String> inputHam = hamRecs.map(new HamLabelRemoveFunction());
		
		// Calculate overall probability of Spam and Ham
		long cntSpamRecords = inputSpam.count();
		long cntHamRecords = inputHam.count();
		float pSpam = (float)cntSpamRecords / (float)(cntSpamRecords + cntHamRecords);
		float pHam = (float)cntHamRecords / (float)(cntSpamRecords + cntHamRecords);	
				
		// Call WordCount to compute frequency of each word
		JavaPairRDD<String, Integer> spamCounts = SparkUtil.countWords(inputSpam);
		JavaPairRDD<String, Integer> hamCounts = SparkUtil.countWords(inputHam);
		
		final Broadcast<Integer> cntWordsSpam = sc.broadcast(SparkUtil.addCounts(spamCounts)); // Total unique words in spam dataset
		final Broadcast<Integer> cntWordsHam = sc.broadcast(SparkUtil.addCounts(hamCounts)); // Total unique words in ham dataset
				
		JavaPairRDD<String, WordFrequency> one = spamCounts.mapValues(new UpdateSpamFrequencyFunction());
		JavaPairRDD<String, WordFrequency> two = hamCounts.mapValues(new UpdateHamFrequencyFunction());
		JavaPairRDD<String,Tuple2<Optional<WordFrequency>,Optional<WordFrequency>>> mergedRDD = one.fullOuterJoin(two);
		
		// Calculate Spam and Ham probabilities for each word
		@SuppressWarnings("serial")
		JavaPairRDD<String, WordFrequency> dictionary = mergedRDD.mapValues(
				new Function<Tuple2<Optional<WordFrequency>,Optional<WordFrequency>>, WordFrequency>() {
			@Override
			public WordFrequency call(
					Tuple2<Optional<WordFrequency>, Optional<WordFrequency>> v1) {

				WordFrequency word = new WordFrequency();
				if (v1._1.isPresent()) {
					word.setCntSpam(v1._1.get().getCntSpam());
				}
				
				if (v1._2.isPresent()) {
					word.setCntHam(v1._2.get().getCntHam());
				}
				word.updateProbabilities(cntWordsSpam.value(), cntWordsHam.value());
				return word;
			}
		});
		dictionary.cache();
		
		// Create a Map from the RDD
		Map<String, WordFrequency> model = dictionary.collectAsMap();
		
		
		/* Classification */
		JavaRDD<String> cleanedTestData = test.filter(new EmptyDataRemoveFunction());
		List<String> testingData = cleanedTestData.collect(); 
		boolean[] testingDataResult = new boolean[testingData.size()];
		boolean[] classifierResult = new boolean[testingData.size()];
		
		int truePos = 0;
		int falsePos = 0;
		int falseNeg = 0;
		int trueNeg = 0;
		
		for (int i = 0; i < testingData.size(); i++) {
			String[] parts = testingData.get(i).split(",");
			
			testingDataResult[i] = "spam".equals(parts[0]);
			classifierResult[i] = isSpam(model, pSpam, pHam, parts[1]);
			
			if (testingDataResult[i]) { // Actual data - Spam
				if (classifierResult[i]) { 
					truePos++; // Spam Spam
				} else {
					falsePos++; // Spam Ham
				}
			} else {
				if (classifierResult[i]) { 
					falseNeg++; // Ham Spam
				} else {
					trueNeg++; // Ham Ham
				}
			}
		}
		
		System.out.println("Training data size : " + training.count());
		System.out.println("Testing data size : " + testingData.size());

		System.out.println("True Positive : " + truePos);
		System.out.println("False Positive : " + falsePos);
		System.out.println("False Negative : " + falseNeg);
		System.out.println("True Negative : " + trueNeg);
		
		// Accuracy = TP + TN / Total
		float accuracy = (float)(truePos + trueNeg) / (float) (truePos + falsePos + falseNeg + trueNeg); 
		
		// Precision = TP / (TP + FP)
		float precision = (float)truePos / (float)(truePos + falsePos);
				
		// Recall = TP / (TP + FN)
		float recall = (float)truePos / (float)(truePos + falseNeg);
		
		System.out.println("Accuracy : " + accuracy);
		System.out.println("Precision : " + precision);
		System.out.println("Recall : " + recall);
		
		sc.close();
	}
	
	/**
	 * 
	 * @param text
	 * @return
	 */
	private static boolean isSpam(Map<String, WordFrequency> model, float pSpam, float pHam, String text) {

		String[] tokens = text.split(" ");
		
		float spamProbability = 1.0f;
		float hamProbability = 1.0f;
		
		for (String token : tokens) {
			if (model.containsKey(token)) {
				spamProbability *= model.get(token).getpSpam();
				hamProbability *= model.get(token).getpHam();				
			}
		}

		spamProbability = spamProbability * pSpam;
		hamProbability = hamProbability * pHam;

		return spamProbability > hamProbability;
	}
}

/**
 * Filters Spam messages from dataset
 */
@SuppressWarnings("serial")
class CleanUpDataFunction implements Function<String, String> {
	private static final String[] specialCharacters = { "#", ";", "\"", "\'", "!", "." };
	private static final String empty = "";
	
	@Override
	public String call(String s) {
		for (String p : specialCharacters) {
			if (s.contains(p)) {
				s = s.replaceAll(p, empty);
			}
		}
		return s.toLowerCase();
	}
}

/**
 * Filters Spam messages from dataset
 */
@SuppressWarnings("serial")
class SpamFilterFunction implements Function<String, Boolean> {
	@Override
	public Boolean call(String s) {
		return s.startsWith("spam");
	}
}

/**
 * Filters Ham messages from dataset
 */
@SuppressWarnings("serial")
class HamFilterFunction implements Function<String, Boolean> {
	@Override
	public Boolean call(String s) {
		return s.startsWith("ham");
	}
}

/**
 * Removes the label
 */
@SuppressWarnings("serial")
class SpamLabelRemoveFunction implements Function<String, String> {
	@Override
	public String call(String s) {
		return s.replaceAll("spam,", "");
	}
}

/**
 * Removes the label
 */
@SuppressWarnings("serial")
class HamLabelRemoveFunction implements Function<String, String> {
	@Override
	public String call(String s) {
		return s.replaceAll("ham,", "");
	}
}

/**
 * Filters empty data
 */
@SuppressWarnings("serial")
class EmptyDataRemoveFunction implements Function<String, Boolean> {
	@Override
	public Boolean call(String s) {
		boolean goodRec = true;
		
		if (s == null || s.trim() == "")  goodRec = false;
		String[] parts = s.split(",");
		if (parts.length != 2) goodRec = false;
		
		return goodRec;
	}
}

