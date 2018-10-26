package com.webscience.crawler.tCrawler.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

@Configuration
public class SocialConfig {
	
	private final String consumerKey = ""; // The application's consumer key
	private final String consumerSecret = ""; // The application's consumer secret
	private final String accessToken = ""; // The access token granted after OAuth authorization
	private final String accessTokenSecret = ""; // The access token secret granted after OAuth authorization

	@Bean
	public twitter4j.conf.Configuration configuration(){
		ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
		configurationBuilder.setDebugEnabled(true)
        .setOAuthConsumerKey(consumerKey)
        .setOAuthConsumerSecret(consumerSecret)
        .setOAuthAccessToken(accessToken)
        .setOAuthAccessTokenSecret(accessTokenSecret);
		configurationBuilder.setJSONStoreEnabled(true);
		return configurationBuilder.build();
	}
	
	@Bean
	public Twitter twitter(twitter4j.conf.Configuration configuration){
		TwitterFactory tf = new TwitterFactory(configuration);
	    Twitter twitter = tf.getInstance();
	    return twitter;
	}
}
