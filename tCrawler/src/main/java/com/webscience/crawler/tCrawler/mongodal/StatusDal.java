package com.webscience.crawler.tCrawler.mongodal;

import java.util.List;
import java.util.Set;

import com.webscience.crawler.tCrawler.model.StatusDoc;

import twitter4j.Place;
import twitter4j.Status;

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
}
