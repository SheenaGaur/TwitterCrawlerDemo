package com.twittergeo.crawler.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.social.twitter.api.Twitter;
import org.springframework.social.twitter.api.impl.TwitterTemplate;

/**
 * TODO: Enter the keys.
 * @author Sheena Gaur
 *
 */
@Configuration
public class CrawlerConfig {
	
	@Bean
	public Twitter twitter(){
		String consumerKey = ""; // The application's consumer key
		String consumerSecret = ""; // The application's consumer secret
		String accessToken = ""; // The access token granted after OAuth authorization
		String accessTokenSecret = ""; // The access token secret granted after OAuth authorization
		Twitter twitter = new TwitterTemplate(consumerKey, consumerSecret, 
				accessToken, accessTokenSecret);
		return twitter;
	}

}
