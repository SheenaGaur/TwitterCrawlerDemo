package com.webscience.crawler.tCrawler.utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.webscience.crawler.tCrawler.mongodal.StatusDalImpl;

import twitter4j.GeoLocation;
import twitter4j.GeoQuery;
import twitter4j.Place;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Trend;
import twitter4j.Trends;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.api.TrendsResources;

/**
 * Service class that interacts with DAL layer to provide REST API
 * specific operations
 * @author Sheena Gaur
 *
 */
@Service
public class RestService {
	
	// Get Twitter Object from Spring Context
	@Autowired
	Twitter twitter;
	
	// Get StatusDalImpl Object from Spring Context
	@Autowired
	private StatusDalImpl statusDalImpl;
	
	// String list of Trends
	private String str[] = null;
	
	// Variable to keep track of the current string being used to get REST data  
	private int currentTrendNumber = 0;
	
	// Getter and Setters of string array 
	public String[] getStr() {
		return str;
	}

	public void setStr(String[] str) {
		this.str = str;
	}

	/**
	 * Method will iterate over list of trend and get tweets that match the current trend string.
	 * This method will be executed very 5 minutes (used to be 15)
	 * @return list of tweets
	 */
	@Scheduled(fixedDelay = 300000)
	public Set<Status> getAllTweets(){
		Set<Status> tweets = new HashSet<>();
		// Create geo-location object for Glasgow
		GeoLocation geolocation = new GeoLocation(55.86515, -4.25763);
		if(null != str){ //If array is not null
			int totalTrends = str.length; // Gett otal number of Strings
			int i = currentTrendNumber; // Current String being looped over
			int loopLimit = i+14; // Max number of Strings that can be looped over in a 15 minute window. Is a Twitter restriction.
			//for(; i < loopLimit && i < totalTrends; i++){ // This was used for the 15 minute window
			for(; i < totalTrends; i++){ // This is used for the 5 minute window
				try {
					System.out.println("Query string:"+str[i]+" with loop limit "+loopLimit);
		            Query query = new Query(str[i]); // Create query with the String as filter
		            query.lang("en"); // Set language
		            query.setCount(100); // Set max number of tweets to be returned. Cannot be greater than 100 and must be specified.
		            query.setGeoCode(geolocation, 10, Query.KILOMETERS); // Set location data and radius.
		            QueryResult result;
		            result = twitter.search(query); // Get tweets from REST API
		            List<Status> res = result.getTweets();
		            tweets.addAll(res);
		            for (Status tweet : res) {
		            	statusDalImpl.addRestStatusTopicFilter(tweet); // Write to database
		                System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
		            }

		        } catch (TwitterException te) {
		            te.printStackTrace();
		            System.out.println("Failed to search tweets: " + te.getMessage());
		        }
			}
			// Set i as the trend number, will be used to update i for the next scheduled loop.
			currentTrendNumber = i;
		}
		return tweets;
	}
	
	/**
	 * Get trend from REST API and add to string list
	 * @return
	 * @throws TwitterException
	 */
	public String[] populateTrends() throws TwitterException{
		Trends tr= twitter.getPlaceTrends(21125); // Get trends for Glasgow. 21125 is the WOEID of Glasgow
		Trend[] t= tr.getTrends(); // Get Trends list from result
		str = new String[t.length];
		// Iterate over lit to get trend string and remove # symbol and " symbol
		for(int i=0;i<t.length;i++)
		{
			if(t[i].getQuery().contains("%23")){
				str[i] = t[i].getQuery().replace("%23", "");
			}
			else if(t[i].getQuery().contains("%22")){
				str[i] = t[i].getQuery().replace("%22", "");
			}
			else{
				str[i] = t[i].getQuery();
			}
			System.out.println(str[i]);
		}
		return str;
	}
	
	/**
	 * Merge all the collections data into single collection.
	 */
	public void mergeAllCollectionData(){
		statusDalImpl.mergeAllCollections();
	}
	
	/**
	 * Method to get Place name from Geolocation.Used for testing hence deprecated.
	 * @return
	 * @throws TwitterException
	 */
	@Deprecated
	public Place getGlasgowPlaceObject() throws TwitterException{
		GeoLocation geolocation = new GeoLocation(55.86515, -4.25763);
        ResponseList<Place> places = twitter.reverseGeoCode(new GeoQuery(geolocation));
        return places.get(0);
	}

}
