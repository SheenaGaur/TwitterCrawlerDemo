package com.webscience.crawler.tCrawler.utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.webscience.crawler.tCrawler.model.StatusDoc;
import com.webscience.crawler.tCrawler.mongodal.StatusDalImpl;

/**
 * This Class is responsible for converting a text to vector
 * @author Sheena Gaur
 *
 */
@Component
public class ConvertTweetToVec {

	// Get StatusDalImpl from Spring context
	@Autowired
	StatusDalImpl statusDalImpl;
	
	// Constant String for RT string
	private final String RETWEET_STRING = "RT ";
	
	//Cleaned out document list
	private List<StatusDoc> docsNewList;
	// Min and max size of tweets
	private int max=0,min=280;
	
	/**
	 * This method clean out the tweet text before they are converted into vectors
	 */
	public void preProcessDocuments(){
		// Gte all document from database
		List<StatusDoc> docsList= statusDalImpl.getAllFromMergedDoc();
		
		docsNewList = new ArrayList<>(); // Initialize object
		
		// Iterate over tweet document list
		for (StatusDoc statusDoc : docsList) {
			String text = statusDoc.getText();
			// If text starts with 'RT ' then remove it.
			if(text.contains(RETWEET_STRING)){
				text = text.substring(3);
			}
			
			int size= text.length(); // Tweet text size
			String[] splitString = text.split(" "); // Split into words
			// Convert array into Arraylist
			List<String> splitStringArrayList = new ArrayList<>(Arrays.asList(splitString));
			// If text size i greater than 280 then remove all user mention words to reduce the size of the tweet. 
			// In this all @ words will be removed.
			splitStringArrayList = size > 280 ? removeATSymbolFromList(splitStringArrayList) : splitStringArrayList;
			// Converts the words back into a sentence
			String joinedString = joinBackString(splitStringArrayList);
			// Remove emoji symbols
			// Code taken from https://stackoverflow.com/questions/49510006/remove-and-other-such-emojis-images-signs-from-java-string.
			joinedString = joinedString.replaceAll("[^\\p{L}\\p{M}\\p{N}\\p{P}\\p{Z}\\p{Cf}\\p{Cs}\\s]","");
			statusDoc.setText(joinedString);
			// Add cleaned object to list
			docsNewList.add(statusDoc);
		}
		// Get max and min size of tweet text
		for (StatusDoc statusDoc : docsNewList) {
			if(statusDoc.getText().length()>max){
				max = statusDoc.getText().length();
			}
			else if(statusDoc.getText().length()<min){
				min= statusDoc.getText().length();
			}
		}
		
		System.out.println("max="+max+" min="+min);
	}
	
	/**
	 * Convert the cleaned document to a integer vector
	 * @return vector
	 */
	public long[][] convertToVectorArray(){
		// Create vector object where doclit length is rows and max text size is column
		long[][] vector = new long[docsNewList.size()][max];
		int count = 0;
		
		// Iterate over documents in list
		for (StatusDoc doc : docsNewList) {
			String text = doc.getText(); //Get tweet text
			// Iterate over the String
			for(int i =0; i<text.length();i++){
				vector[count][i] = text.charAt(i); // Convert each character to its ASCII value and add to array.
			}
			// Set last column as tweet ID 
			vector[count][max-1] = doc.getId();
			count++;
		}
		// Return vector
		return vector;
	}
	
	/**
	 * Method to remove @ symbol words from list
	 * @param spliStringArrayList
	 * @return
	 */
	private List<String> removeATSymbolFromList(List<String> spliStringArrayList){
		// Iterate over list
		Iterator<String> itr = spliStringArrayList.iterator();
		while(itr.hasNext()){
			String text = itr.next(); // Get next object
			if(text.startsWith("@")){ // Check if String starts with @ symbol
				itr.remove(); // Remove it.
			}
		}	
		return spliStringArrayList;
	}
	
	/**
	 * Convert String list to single string separated by space
	 * @param splitStringArrayList
	 * @return
	 */
	private String joinBackString(List<String> splitStringArrayList){
		String str = new String(""); // Initialise
		// Iterate over string list
		for (String string : splitStringArrayList) {
			str = str + string + " "; // Combine with space.
		}
		return str;
	}
}
