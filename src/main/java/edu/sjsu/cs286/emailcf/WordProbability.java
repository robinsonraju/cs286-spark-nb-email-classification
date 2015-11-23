package edu.sjsu.cs286.emailcf;

/**
 * This class holds the probabilities for a word. 
 * 
**/
public class WordProbability {
	// The word
	private String word;
	
	// Number of times the word appears in Spam messages
	private int cntSpam; 
	
	// Number of times the word appears in Ham messages
	private int cntHam; 
	
	
	
	// Probability that the word is Spam
	// (1 + cntSpam) / 
	// [(total unique words in Spam dataset) + (total unique words in All dataset)]
	private float pSpam; 
	
	// Probability that the word is Ham
	// (1 + cntHam) / 
	// [(total unique words in Ham dataset) + (total unique words in All dataset)]
	private float pHam;
	

	// Create a word, initialize all vars to 0
	public WordProbability(String s) {
		word = s;
		cntSpam = 0;
		cntHam = 0;
		pSpam = 0.0f;
		pHam = 0.0f;
	}

	public String getWord() {
		return word;
	}


	public void setWord(String word) {
		this.word = word;
	}


	public int getCntSpam() {
		return cntSpam;
	}


	public void setCntSpam(int cntSpam) {
		this.cntSpam = cntSpam;
	}


	public int getCntHam() {
		return cntHam;
	}


	public void setCntHam(int cntHam) {
		this.cntHam = cntHam;
	}


	public float getpSpam() {
		return pSpam;
	}

	/*
	 * Calculate probability of the word being in Spam using 
	 * cntWordsSpam and cntWordsAll 
	 */
	public void setpSpam(int cntWordsSpam, int cntWordsAll) {
		this.pSpam = (1 + cntSpam) / (cntWordsSpam + cntWordsAll);
	}


	public float getpHam() {
		return pHam;
	}

	/*
	 * Calculate probability of the word being in Ham using 
	 * cntWordsHam and cntWordsAll 
	 */
	public void setpHam(int cntWordsHam, int cntWordsAll) {
		this.pHam = (1 + cntHam) / (cntWordsHam + cntWordsAll);
	}
}
