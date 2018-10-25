package com.webscience.crawler.tCrawler.utilities;

import java.util.ArrayList;
import java.util.List;

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

@Service
public class StreamService {

	@Autowired
	private Configuration config;
	
	@Autowired
	private StatusDalImpl statusDalImpl;
	
	private TwitterStream twitterStreamGeneric, twitterStreamLocationSpecific, twitterStreamTopicSpecific;

	public List<Status> streamApi(){
		List<Status> allStatus =new ArrayList<>(); 
		twitterStreamGeneric = new TwitterStreamFactory(config).getInstance();
		System.out.println("Begun Streamming...");
		StatusListener listener =new StatusListener() {
			
			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
				
			}
			
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
				
			}
			
			@Override
			public void onStatus(Status status) {
				System.out.println(status.getId() + " : " + status.getUser().getName() + " : "+status.getText());
				statusDalImpl.addStreamStatus(status);
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
		
		
		
		twitterStreamGeneric.addListener(listener);
		
		twitterStreamGeneric.sample("en");
		
		return allStatus;
	}
	
	public List<Status> streamLocationApi(){
		List<Status> allStatus =new ArrayList<>(); 
		twitterStreamLocationSpecific = new TwitterStreamFactory(config).getInstance();
		System.out.println("Begun Streamming...");
		StatusListener listener =new StatusListener() {
			
			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
				
			}
			
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
				
			}
			
			@Override
			public void onStatus(Status status) {
				System.out.println(status.getId() + " : " + status.getUser().getName() + " : "+status.getText());
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

		double[][] locations = {{-5,55},{-4,56}};
		FilterQuery filterQuery =new FilterQuery();
		filterQuery.language("en");
		filterQuery.locations(locations);
		
		twitterStreamLocationSpecific.addListener(listener);
		
		twitterStreamLocationSpecific.filter(filterQuery);
		
		return allStatus;
	}
	
	public List<Status> streamTopicApi(String[] topic){
		List<Status> allStatus =new ArrayList<>(); 
		twitterStreamTopicSpecific = new TwitterStreamFactory(config).getInstance();
		System.out.println("Begun Streamming...");
		StatusListener listener =new StatusListener() {
			
			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
				
			}
			
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
				
			}
			
			@Override
			public void onStatus(Status status) {
				System.out.println(status.getId() + " : " + status.getUser().getName() + " : "+status.getText());
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
		
		FilterQuery filterQuery =new FilterQuery();
		filterQuery.language("en");
		filterQuery.track(topic);
		
		twitterStreamTopicSpecific.addListener(listener);
		
		twitterStreamTopicSpecific.filter(filterQuery);
		
		return allStatus;
	}
	
	@Scheduled(initialDelay = 3600000, fixedDelay = 3600000)
	public void shutdownGenericStream(){
		System.out.println("ShutDown called: shutdownGenericStream");
		if(null != twitterStreamGeneric){
			twitterStreamGeneric.shutdown();
			twitterStreamGeneric = null;
		}
		System.exit(0);
	}
	
	@Scheduled(initialDelay = 3600000, fixedDelay = 3600000)
	public void shutDownLocationSpecficStream(){
		System.out.println("ShutDown called: shutDownLocationSpecficStream");
		if(null != twitterStreamLocationSpecific){
			twitterStreamLocationSpecific.shutdown();
			twitterStreamLocationSpecific = null;
		}
		
	}
	
	@Scheduled(initialDelay = 3600000, fixedDelay = 3600000)
	public void shutDownTopicSpecificStream(){
		System.out.println("ShutDown called: shutDownTopicSpecificStream");
		if(null != twitterStreamTopicSpecific){
			twitterStreamTopicSpecific.shutdown();
			twitterStreamTopicSpecific = null;
		}
	}
}