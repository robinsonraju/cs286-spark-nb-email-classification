package edu.sjsu.cs286.emailcf.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 
 * Simple classifier that uses Naive Bayes to classify spam or ham
 *
 */
public class JavaEmailClassifier {

	private static final String SPAM = "spam";
	private static final String HAM = "ham";
	private static final String[] specialCharacters = { ",", "#", ";", "\"", "\'", "!", "." };
	private static final String empty = "";
	
	private static Map<String, WordFrequency> wordDictionary = new HashMap<String, WordFrequency>();
	private static int cntWordsSpam = 0;// Total unique words in spam dataset
	private static int cntWordsHam = 0; // Total unique words in ham dataset
	private static float pSpam = 0.0f; // Probability of Spam
	private static float pHam = 0.0f; // Probability of Ham
	
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
		String percTraining = "70";
		
		// Read data into memory
		List<String> inputData = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				inputData.add(line);
			}
		}
		Collections.shuffle(inputData);
		
		// Train
		List<String> trainingData = getSublist(inputData, percTraining, true); 
		train(trainingData);
		
		// Classify
		List<String> testingData = getSublist(inputData, percTraining, false); 
		boolean[] testingDataResult = new boolean[testingData.size()];
		boolean[] classifierResult = new boolean[testingData.size()];
		
		int truePos = 0;
		int falsePos = 0;
		int falseNeg = 0;
		int trueNeg = 0;
		
		for (int i = 0; i < testingData.size(); i++) {
			String[] parts = testingData.get(i).split(",");
			if (parts.length!=2) continue; 
			
			testingDataResult[i] = SPAM.equals(parts[0]);
			classifierResult[i] = isSpam(parts[1]);
			
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

		System.out.println("Training data size : " + trainingData.size());
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
	
	}
	
	
	/**
	 * 
	 * @param trainingData
	 */
	private static void train(List<String> trainingData ) {
		Map<String, WordFrequency> spamWords = new HashMap<String, WordFrequency>();
		Map<String, WordFrequency> hamWords = new HashMap<String, WordFrequency>();
		int cntSpamRecords = 0;
		int cntHamRecords = 0;
		
		// WordCount
		for (String line : trainingData) {
			String[] parts = line.split(",");
			if(SPAM.equals(parts[0])) {
				updateWordCount(spamWords, parts[1], true);
				cntSpamRecords++;
			} else if(HAM.equals(parts[0])) {
				updateWordCount(hamWords, parts[1], false);
				cntHamRecords++;
			} else {
				// Ignore - bad data
			}
		}
		System.out.println("Count Spam : " + cntSpamRecords);
		System.out.println("Count Ham : " + cntHamRecords);
		
		pSpam = (float)cntSpamRecords / (float)(cntSpamRecords + cntHamRecords);
		pHam = (float)cntHamRecords / (float)(cntSpamRecords + cntHamRecords);	
		
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
	 * 
	 * @param text
	 * @return
	 */
	private static boolean isSpam(String text) {
		String[] tokens = text.split(" ");
		
		float spamProbability = 1.0f;
		float hamProbability = 1.0f;
		
		for (String token : tokens) {
			token = normalize(token);
			if (wordDictionary.containsKey(token)) {
				System.out.println(wordDictionary.get(token).toString());
				spamProbability *= wordDictionary.get(token).getpSpam();
				hamProbability *= wordDictionary.get(token).getpHam();				
			}
		}

		spamProbability = spamProbability * pSpam;
		hamProbability = hamProbability * pHam;

		return spamProbability > hamProbability;
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
