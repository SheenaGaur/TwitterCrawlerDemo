package com.webscience.crawler.tCrawler.utilities;

import java.util.ArrayList;
import java.util.List;

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

@Service
public class RestService {
	
	@Autowired
	Twitter twitter;
	
	@Autowired
	private StatusDalImpl statusDalImpl;
	
	private String str[] = null;
	
	private int currentTrendNumber = 0;
	
	public String[] getStr() {
		return str;
	}

	public void setStr(String[] str) {
		this.str = str;
	}

	@Scheduled(fixedDelay = 900000)
	public void getAllTweets(){
		List<Status> tweets = new ArrayList<>();
		GeoLocation geolocation = new GeoLocation(55.86515, -4.25763);
		if(null != str){
			int totalTrends = str.length;
			int i = currentTrendNumber;
			int loopLimit = i+14;
			for(; i < loopLimit && i < totalTrends; i++){
				
				try {
					System.out.println("Query string:"+str[i]+" with loop limit "+loopLimit);
		            Query query = new Query(str[i]);
		            query.lang("en");
		            query.setCount(100);
		            query.setGeoCode(geolocation, 10, Query.KILOMETERS);
		            QueryResult result;
		            result = twitter.search(query);

		            tweets = result.getTweets();
		            for (Status tweet : tweets) {
		            	statusDalImpl.addRestStatusTopicFilter(tweet);
		                System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
		            }

		        } catch (TwitterException te) {
		            te.printStackTrace();
		            System.out.println("Failed to search tweets: " + te.getMessage());
		        }
			}
			currentTrendNumber = i;
		}
		
	}
	
	public String[] populateTrends() throws TwitterException{
		Trends tr= twitter.getPlaceTrends(21125);
		Trend[] t= tr.getTrends();
		str = new String[t.length];
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
	
	
	public void mergeAllCollectionData(){
		statusDalImpl.mergeAllCollections();
	}
	
	@Deprecated
	public Place getGlasgowPlaceObject() throws TwitterException{
		GeoLocation geolocation = new GeoLocation(55.86515, -4.25763);
        ResponseList<Place> places = twitter.reverseGeoCode(new GeoQuery(geolocation));
        return places.get(0);
	}

}
