package com.webscience.crawler.tCrawler.mongodal;

import java.util.List;
import java.util.Set;

import com.tumblr.jumblr.types.Post;
import com.webscience.crawler.tCrawler.model.StatusDoc;
import com.webscience.crawler.tCrawler.model.TumblrDoc;

import twitter4j.Place;
import twitter4j.Status;

/**
 * Interface for Data Access layer class that directly interacts with the database.
 * Method descriptions in the inherited class.
 * @author Sheena Gaur
 *
 */
public interface StatusDal {
	
	Status addStreamStatus(Status status);
	
	Status addStreamStatusTopicFilter(Status status);
	
	Status addStreamStatusLocationFilter(Status status);
	
	Status addRestStatusTopicFilter(Status status);
	
	void findAllDocs();
	
	void getCountGeoTagged();
	
	long getCountGeoAndPlace();
	
	long getCountGeoOnly();
	
	long getCountPlaceOnly();
	
	void getRetweetCount();
	
	void getQuotedCount();
	
	List<StatusDoc> getGlasgowTaggedDocs();
	
	List<StatusDoc> getOneHourGeneralStreamData();
	
	List<StatusDoc> getAllIdFromCollections();
	
	void mergeAllCollections();
	
	List<StatusDoc> getAllFromMergedDoc();
	
	List<StatusDoc> getAllDataMergedCollection();
	
	void saveTumblrData(Post post);
	
	void saveTumblrList(List<Post> posts);
	
	void countAllTumblrDocs();
	
	void getMaxNotesForPost();
	
	List<TumblrDoc> getMaxOccuredTumblrBlog();
	
	List<StatusDoc> getGeoAndPlaceDataFromMergedList();
	
	List<StatusDoc> getHalfGeoAndPlaceDataFromMergedList();
	
	void deleteAllFromAllCollections();
	
	long getNumCount(float num);
}
