package com.webscience.crawler.tCrawler.utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.webscience.crawler.tCrawler.mongodal.StatusDalImpl;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;

/**
 * Service class that interacts with DAL layer to provide Stream API
 * specific operations
 * @author Sheena Gaur
 *
 */
@Service
public class StreamService {

	// Get Configuration Object from Spring Context
	@Autowired
	private Configuration config;
	
	// Get StatusDalImpl Object from Spring Context
	@Autowired
	private StatusDalImpl statusDalImpl;
	
	// Three stream objects for three types of streams
	private TwitterStream twitterStreamGeneric, twitterStreamLocationSpecific, twitterStreamTopicSpecific;

	/**
	 * Call the GardenHose Stream to get all data with no filter.
	 * 
	 * @return Tweets
	 */
	public Set<Status> streamApi(){
		Set<Status> allStatus =new HashSet<>(); 
		// Create TwitterStream object from Configuration object.
		twitterStreamGeneric = new TwitterStreamFactory(config).getInstance();
		System.out.println("Begun Streamming...");
		// Create Listener object to listen for tweets. It is an interface so its method must be overridden.
		StatusListener listener =new StatusListener() {
			
			/**
			 * Get exception notification
			 */
			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
				
			}
			
			/**
			 * Get track limit notification
			 */
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
				
			}
			
			/**
			 * Get Tweet data from Stream when it is posted.
			 */
			@Override
			public void onStatus(Status status) {
				System.out.println(status.getId() + " : " + status.getUser().getName() + " : "+status.getText());
				statusDalImpl.addStreamStatus(status); // Add to database
				allStatus.add(status);
				
			}
			
			/**
			 * Get stall notification
			 */
			@Override
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
				
			}
			
			/**
			 * Get scrubing of geo-information notification
			 */
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
				
			}
			
			/**
			 * Get delete notification
			 */
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
				
			}
		};
		
		// Add listener to Stream
		twitterStreamGeneric.addListener(listener);
		// Set language and begin streaming data
		twitterStreamGeneric.sample("en");
		
		return allStatus;
	}
	
	/**
	 * Call Stream API to get location specific tweets
	 * @return tweets
	 */
	public Set<Status> streamLocationApi(){
		Set<Status> allStatus =new HashSet<>(); 
		// Create TwitterStream object from Configuration object.
		twitterStreamLocationSpecific = new TwitterStreamFactory(config).getInstance();
		System.out.println("Begun Streamming...");
		// Create Listener object to listen for tweets. It is an interface so its method must be overridden.
		StatusListener listener =new StatusListener() {
			
			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
				
			}
			
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
				
			}
			
			/**
			 * Get Tweet data from Stream when it is posted.
			 */
			@Override
			public void onStatus(Status status) {
				System.out.println(status.getId() + " : " + status.getUser().getName() + " : "+status.getText());
				// Add to database
				statusDalImpl.addStreamStatusLocationFilter(status);
				allStatus.add(status);
				
			}
			
			@Override
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
				
			}
			
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
				
			}
			
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
				
			}
		};

		// Set geo-locatoin box i.e. bottom-left and top-right
		double[][] locations = {{-4.5,55.7},{-4,56}};
		// Create Filter object and set language and geo-location parameters 
		FilterQuery filterQuery =new FilterQuery();
		filterQuery.language("en");
		filterQuery.locations(locations);
		// Add listener
		twitterStreamLocationSpecific.addListener(listener);
		// Begin streaming with the given filter
		twitterStreamLocationSpecific.filter(filterQuery);
		
		return allStatus;
	}
	
	/**
	 * Call Stream API to get Topic specific tweets
	 * @param topic
	 * @return tweets
	 */
	public Set<Status> streamTopicApi(String[] topic){
		Set<Status> allStatus =new HashSet<>();
		// Create TwitterStream object from Configuration object.
		twitterStreamTopicSpecific = new TwitterStreamFactory(config).getInstance();
		System.out.println("Begun Streamming...");
		// Create Listener object.
		StatusListener listener =new StatusListener() {
			
			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
				
			}
			
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
				
			}
			
			/**
			 * Get Tweet data from Stream when it is posted.
			 */
			@Override
			public void onStatus(Status status) {
				System.out.println(status.getId() + " : " + status.getUser().getName() + " : "+status.getText());
				// Add to database
				statusDalImpl.addStreamStatusTopicFilter(status);
				allStatus.add(status);
				
			}
			
			@Override
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
				
			}
			
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
				
			}
			
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
				
			}
		};
		// Create Filter object and set language and topic filter parameters 
		FilterQuery filterQuery =new FilterQuery();
		filterQuery.language("en");
		filterQuery.track(topic);
		// Add listener
		twitterStreamTopicSpecific.addListener(listener);
		// Begin streaming.
		twitterStreamTopicSpecific.filter(filterQuery);
		
		return allStatus;
	}
	
	/**
	 * ShutDown gardenhose stream after 5 minutes (was 1hour) and exit application
	 */
	//@Scheduled(initialDelay = 3600000, fixedDelay = 3600000)
	@Scheduled(initialDelay = 300000, fixedDelay = 300000)
	public void shutdownGenericStream(){
		System.out.println("ShutDown called: shutdownGenericStream");
		if(null != twitterStreamGeneric){ // If object not null
			twitterStreamGeneric.shutdown(); // call shutdown
			twitterStreamGeneric = null; // set object to null
		}
		System.exit(0); // exit application
	}
	
	/**
	 * ShutDown location stream after 5 minutes (was 1hour) and exit application
	 */
	//@Scheduled(initialDelay = 3600000, fixedDelay = 3600000)
	@Scheduled(initialDelay = 300000, fixedDelay = 300000)
	public void shutDownLocationSpecficStream(){
		System.out.println("ShutDown called: shutDownLocationSpecficStream");
		if(null != twitterStreamLocationSpecific){ // If object not null
			twitterStreamLocationSpecific.shutdown(); // shutdown stream
			twitterStreamLocationSpecific = null; // Set object to null.
		}
		
	}
	
	/**
	 * ShutDown topic stream after 5 minutes (was 1hour) and exit application
	 */
	//@Scheduled(initialDelay = 3600000, fixedDelay = 3600000)
	@Scheduled(initialDelay = 300000, fixedDelay = 300000)
	public void shutDownTopicSpecificStream(){
		System.out.println("ShutDown called: shutDownTopicSpecificStream");
		if(null != twitterStreamTopicSpecific){ // If object not null
			twitterStreamTopicSpecific.shutdown(); // Shutdown stream
			twitterStreamTopicSpecific = null; // et object to null.
		}
	}
}