package edu.sjsu.cs286.emailcf.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 
 * @author rraju
 * Simple classifier that uses Naive Bayes to classify spam or ham
 *
 */
public class JavaTextClassifier {

	private static final String SPAM = "spam";
	private static final String HAM = "ham";
	private static final String[] specialCharacters = { ",", "#", ";", "\"", "\'", "!", "." };
	private static final String empty = "";
	
	private static Map<String, WordFrequency> wordDictionary = new HashMap<String, WordFrequency>();
	private static int cntWordsSpam = 0;// Total unique words in spam dataset
	private static int cntWordsHam = 0; // Total unique words in ham dataset
	
	
	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		// Input
		/*
		String inputFile = args[0];
		String percTraining = args[1];
		 */
		
		String inputFile = "src/main/resources/subjects.csv";
		String percTraining = "80";
		
		// Read data into memory
		List<String> inputData = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				inputData.add(line);
			}
		}
		
		// Train
		List<String> trainingData = getSublist(inputData, percTraining, true); 
		train(trainingData);

		System.out.println(wordDictionary.size());
		System.out.println(cntWordsSpam);
		System.out.println(cntWordsHam);
		
		// Classify
		List<String> testingData = getSublist(inputData, percTraining, false); 
		

	}
	
	
	/**
	 * 
	 * @param trainingData
	 */
	private static void train(List<String> trainingData ) {
		Map<String, WordFrequency> spamWords = new HashMap<String, WordFrequency>();
		Map<String, WordFrequency> hamWords = new HashMap<String, WordFrequency>();
		
		// WordCount
		for (String line : trainingData) {
			String[] parts = line.split(",");
			if(SPAM.equals(parts[0])) {
				updateWordCount(spamWords, parts[1], true);
			} else if(HAM.equals(parts[0])) {
				updateWordCount(hamWords, parts[1], false);
			} else {
				// Ignore - bad data
			}
		}
		
		cntWordsSpam = spamWords.size();
		cntWordsHam = hamWords.size();
		
		// Merge Maps
		for (String key : spamWords.keySet()) {
			if (hamWords.containsKey(key)) {
				WordFrequency wordFreqFromHam = hamWords.get(key);
				wordFreqFromHam.setCntSpam(spamWords.get(key).getCntSpam());
			} else {
				hamWords.put(key, spamWords.get(key));
			}
		}
		wordDictionary.putAll(hamWords);
		
		// Update probabilities
		for (String key : wordDictionary.keySet()) {
			WordFrequency wordFreq = wordDictionary.get(key);
			wordFreq.updateProbabilities(cntWordsSpam, cntWordsHam);
		}
	
	}

	/**
	 * Create a hashmap of words with corresponding frequencies.
	 * @param words
	 * @param text
	 * @param spam
	 */
	private static void updateWordCount(Map<String, WordFrequency> words, String text, boolean spam){
		
		String[] tokens = text.split(" ");

		for (String token : tokens) {
			token = normalize(token);
			
			if (!empty.equals(token.trim())) {
				if (words.containsKey(token)) {
					WordFrequency wordFreq = words.get(token);
					wordFreq.updateCounts(spam);
				} else {
					WordFrequency wordFreq = new WordFrequency(token, spam);
					words.put(token, wordFreq);
				}
			}
		}
	}
	
	/**
	 * Normalize the String. 
	 * Take out special characters, convert to lower case. 
	 * @param s
	 * @return
	 */
	private static String normalize(String s) {
		for (String p : specialCharacters) {
			if (s.contains(p)) {
				s = s.replaceAll(p, empty);
			}
		}
		return s.toLowerCase();
	}
	
	/**
	 * Return a sublist based on the input percentage
	 * Training data is expected to be the top x% and testing (100-x) %
	 * @param inputData
	 * @param percTraining
	 * @param training
	 * @return
	 */
	private static List<String> getSublist (List<String> inputData, String percTraining, boolean training) {
		int percentage = Integer.valueOf(percTraining).intValue();
		int index = inputData.size() * percentage / 100; 
		
		if (training) {
			return inputData.subList(0, index);
		} else {
			return inputData.subList(index, inputData.size());
		}
	}
}
