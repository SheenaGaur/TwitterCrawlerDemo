package com.twittergeo.crawler.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.social.twitter.api.StreamDeleteEvent;
import org.springframework.social.twitter.api.StreamListener;
import org.springframework.social.twitter.api.StreamWarningEvent;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.stereotype.Service;

/**
 * 
 * @author Sheena Gaur
 *
 */
@Service
public class TwitterStreamingService {

	public List<StreamListener> beginStreaming() throws InterruptedException{

        List<Tweet> tweets = new ArrayList<>();
        final File file = new File("tweets.txt");
        List<StreamListener> listeners = new ArrayList<StreamListener>();
        
        StreamListener streamListener = new StreamListener() {

            @Override
            public void onWarning(StreamWarningEvent warningEvent) {
                System.out.println(warningEvent.getMessage());

            }

            @Override
            public void onTweet(Tweet tweet) {
                System.out.println(tweet.getUser().getName() + " : " + tweet.getText());
               
                tweets.add(tweet);
                try {
                    final BufferedWriter writer  = new BufferedWriter(new FileWriter(file));
	                writer.write(tweet.toString());
	                writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }

            @Override
            public void onLimit(int numberOfLimitedTweets) {
            	System.out.println("Got track limitation notice:" + numberOfLimitedTweets);

            }

            @Override
            public void onDelete(StreamDeleteEvent deleteEvent) {
            	System.out.println("Got a status deletion notice id:" + deleteEvent.getTweetId());

            }
        };

        listeners.add(streamListener);
        return listeners;
    }
}
