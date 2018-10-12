package com.twittergeo.crawler.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.social.twitter.api.StreamListener;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.social.twitter.api.Twitter;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.twittergeo.crawler.service.TwitterStreamingService;

/**
 * 
 * @author Sheena Gaur
 *
 */
@RestController
@RequestMapping("proj/tweet")
public class TwitterCrawlerController {

	@Autowired
	private Twitter twitter;
	
	@Autowired
	private TwitterStreamingService twitterStreamingService;
	
	@RequestMapping(value = "{hashTag}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public List<Tweet> getTweets(@PathVariable final String hashTag){
		//twitter.searchOperations().getLocalTrends(21125);
		return twitter.searchOperations().search(hashTag).getTweets();
	}
	
	@RequestMapping(value = "/timeline", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public List<Tweet> getTimeLine(){
		return twitter.timelineOperations().getUserTimeline("gaur_sheena");
	}
	
	@RequestMapping(value = "/stream/{search}")
	public String streamTweet(@PathVariable final String search) throws InterruptedException{
		List<StreamListener> listeners= twitterStreamingService.beginStreaming();
		twitter.streamingOperations().filter(search, listeners);
	    return "tweets";
	}
	
}
