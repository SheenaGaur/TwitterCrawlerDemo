package com.webscience.crawler.tCrawler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.webscience.crawler.tCrawler.model.StatusDoc;
import com.webscience.crawler.tCrawler.utilities.*;

import info.debatty.java.lsh.LSHMinHash;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.TwitterException;

@SpringBootApplication
@EnableScheduling
public class SocialMediaCrawlerApplication {

	public static void main(String[] args) throws IOException {
		ApplicationContext context = SpringApplication.run(SocialMediaCrawlerApplication.class, args);
		StreamService streamService = context.getBean(StreamService.class);
		RestService restService = context.getBean(RestService.class);
		TweetStatusService tweetStatusService = context.getBean(TweetStatusService.class);
		TumblrService tumblrService = context.getBean(TumblrService.class);
		
		// To populate the database. This method call is no longer required.
		// makeStreamAndRestCalls(streamService, restService);

		// To get counts of data.
		// performDataCount(tweetStatusService);

		// write to CSV file
		//writeToCSV(tweetStatusService);
		
		// Populate database with Tumblr posts
		//tumblrService.getTumblrTagPostDetails();
		//writeDuplicateDataToTemp(tumblrService.getDuplicateData());
		
		// Get data counts
		getTumblrDataCounts(tumblrService);

		System.exit(0);
	}

	static void performDataCount(TweetStatusService tweetStatusService) {
		tweetStatusService.getDocCount();
		tweetStatusService.getGeoTaggedCount();
		tweetStatusService.getRetweetsAndQuotesCount();

		try {
			List<StatusDoc> geoTaggedList = tweetStatusService.getGlasgowTaggedDocs();
			tweetStatusService.getOverlapWithGeneralStream(geoTaggedList);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		tweetStatusService.getAllRedundantData();
	}

	/**
	 * This method is no longer required to be called a the required data has
	 * already been collected. It makes a call to the Stream API and REST API.
	 * It then combines all the data collected for analysis.
	 * 
	 * @param streamService
	 * @param restService
	 */
	static void makeStreamAndRestCalls(StreamService streamService, RestService restService) {
		// 1. Stream Data without filter for one hour.
		// List<Status> status = streamService.streamApi();

		// Rest calls
		try {
			// 2. Make a REST call to get current trends in Glasgow.
			String trends[] = restService.populateTrends();
			String[] topics = { "glasgow", "football", "weather", "storm", "brexit" };
			String[] filterKeywords = combineStringArrays(topics, trends);
			// Stream data using current trends and geo-location of Glasgow.
			streamService.streamTopicApi(filterKeywords);
			streamService.streamLocationApi();

			// 3. REST calls to search for given keywords.
			String[] currentTrends = restService.getStr();
			// Increase the search keywords to 56 to gets searches for 1 hr.
			// Some of these
			// keywords may match the trends.
			String[] searchWords = { "glasgow", "football", "weather", "storm", "brexit", "love", "hate", "trump",
					"winter", "puja", "lol", "best", "festival", "job", "dussehra", "crazy", "dream", "game",
					"halloween", "venom", "christmas", "player", "marvel", "batman", "cold", "tweet", "britain",
					"words" };
			String[] result = combineStringArrays(currentTrends, searchWords);
			restService.setStr(result);
			restService.getAllTweets();

			// 4. Merge all document received to provide data for hashing
			// algorithm.
			restService.mergeAllCollectionData();
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static String[] combineStringArrays(String[] ar1, String[] ar2) {

		ArrayList<String> list1 = new ArrayList<>(Arrays.asList(ar1));
		ArrayList<String> list2 = new ArrayList<>(Arrays.asList(ar2));
		list2.addAll(list1);
		String[] result = list2.toArray(new String[0]);

		return result;
	}

	static void writeToCSV(TweetStatusService tweetStatusService) throws IOException {
		List<StatusDoc> tweets = tweetStatusService.getAllMergedTweets();
		System.out.println("Data received from database.");
		File file = new File("./localityprocessing/tweets.txt");
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		for (StatusDoc statusDoc : tweets) {
			//writer.write(statusDoc.getId() + "\t" + statusDoc.getText());
			writer.write(statusDoc.getText());
			writer.newLine();
		}
		System.out.println("Written to file.");
		writer.close();
	}
	
	static void writeDuplicateDataToTemp(int duplicateData) throws IOException{
		File file = new File("./tempData/PostsNumber.txt");
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
			writer.write(String.valueOf(duplicateData));
			writer.newLine();
		System.out.println("Written to file.");
		writer.close();	
	}
	
	static void getTumblrDataCounts(TumblrService tumblrService) throws IOException{
		tumblrService.getTotalData();
		tumblrService.getMaxAuthorCount();
		tumblrService.getBlogWithMaxNotes();
		File file = new File("./tempData/PostsNumber.txt");
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String duplicateData = reader.readLine();
		System.out.println("Duplicate data count found while getting Tumblr posts:" + duplicateData);
		reader.close();
	}
}
