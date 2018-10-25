package com.webscience.crawler.tCrawler.utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.webscience.crawler.tCrawler.model.StatusDoc;
import com.webscience.crawler.tCrawler.mongodal.StatusDalImpl;

import twitter4j.Place;

@Service
public class TweetStatusService {
	
	@Autowired
	StatusDalImpl statusDalImpl;
	
	public void getDocCount(){
		statusDalImpl.findAllDocs();
	}
	
	public void getGeoTaggedCount(){
		statusDalImpl.getCountGeoTagged();
	}
	
	public void getRetweetsAndQuotesCount(){
		statusDalImpl.getRetweetCount();
		statusDalImpl.getQuotedCount();
	}
	
	public List<StatusDoc> getGlasgowTaggedDocs(){
		List<StatusDoc> statuses = statusDalImpl.getGlasgowTaggedDocs();
		System.out.println("GeoTagged data:"+statuses.size());
		return statuses;
	}
	
	public void getOverlapWithGeneralStream(List<StatusDoc> geoTaggedList){
		List<Long> generalStreamIdList = getIdFromList(statusDalImpl.getOneHourGeneralStreamData());
		List<Long> geoTaggedIdList = getIdFromList(geoTaggedList);
		generalStreamIdList.retainAll(geoTaggedIdList);
		System.out.println("Overlap with 1% data:"+generalStreamIdList.size());
	}

	public List<Long> getIdFromList(List<StatusDoc> list){
		List<Long> idList = new ArrayList<>();
		for (StatusDoc statusDoc : list) {
			idList.add(statusDoc.getId());
		}
		return idList;
	}
	
	public void getAllRedundantData(){
		List<Long> generalStreamIdList = getIdFromList(statusDalImpl.getOneHourGeneralStreamData());
		List<Long> allDataIdList = getIdFromList(statusDalImpl.getAllIdFromCollections());
		allDataIdList.addAll(generalStreamIdList);
		HashMap<Long, Integer> idCountMap = new HashMap<>();
		for (Long long1 : allDataIdList) {
			if(idCountMap.containsKey(long1)){
				idCountMap.put(long1, idCountMap.get(long1) + 1);
			}else{
				idCountMap.put(long1, 0);
			}
		}
		
		// Get total count. 
		// Code can be found at : https://stackoverflow.com/questions/1066589/iterate-through-a-hashmap
		int count = 0;
		Iterator<Entry<Long, Integer>> itr = idCountMap.entrySet().iterator();
	    while (itr.hasNext()) {
	        Map.Entry<Long,Integer> pair = (Entry<Long, Integer>)itr.next();
	        count = count + pair.getValue();
	        itr.remove(); // to avoid a ConcurrentModificationException
	    }
		
	    System.out.println("Total redundant data:"+count);
	}
}
