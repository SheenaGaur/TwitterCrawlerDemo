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

import com.webscience.crawler.tCrawler.model.ClusterGeoTag;
import com.webscience.crawler.tCrawler.model.StatusDoc;
import com.webscience.crawler.tCrawler.mongodal.StatusDalImpl;

import twitter4j.GeoLocation;
import twitter4j.Place;

/**
 * Service class that interacts with DAL layer to provide general tweet
 * specific operations
 * @author Sheena Gaur
 *
 */
@Service
public class TweetStatusService {
	
	// Get StatusDalImpl Object from Spring Context
	@Autowired
	StatusDalImpl statusDalImpl;
	
	/**
	 * Get total document count.
	 */
	public void getDocCount(){
		statusDalImpl.findAllDocs();
	}
	
	/**
	 * Get total count of geo-tagged data
	 */
	public void getGeoTaggedCount(){
		statusDalImpl.getCountGeoTagged();
	}
	
	/**
	 * Get total count of re-tweets and quotes
	 */
	public void getRetweetsAndQuotesCount(){
		statusDalImpl.getRetweetCount(); // Get retweet count
		statusDalImpl.getQuotedCount(); // Get quote count
	}
	
	/**
	 * Get geo-tagged data count from Glasgow 
	 * @return
	 */
	public List<StatusDoc> getGlasgowTaggedDocs(){
		List<StatusDoc> statuses = statusDalImpl.getGlasgowTaggedDocs();
		System.out.println("GeoTagged data from Glasgow:"+statuses.size());
		return statuses;
	}
	
	/**
	 * Get overlap count with gardenhose document list
	 * @param geoTaggedList
	 */
	public void getOverlapWithGeneralStream(List<StatusDoc> geoTaggedList){
		List<Long> generalStreamIdList = getIdFromList(statusDalImpl.getOneHourGeneralStreamData()); // Get all Id's from collections
		List<Long> geoTaggedIdList = getIdFromList(geoTaggedList); // Get geo-tagged is list
		generalStreamIdList.retainAll(geoTaggedIdList); // Keep only that id that is in the geoTaggedIdList list.
		System.out.println("Overlap with 1% data:"+generalStreamIdList.size());
	}

	/**
	 * Form a list containing only tweet Id' from tweets list
	 * @param list
	 * @return
	 */
	public List<Long> getIdFromList(List<StatusDoc> list){
		List<Long> idList = new ArrayList<>();
		for (StatusDoc statusDoc : list) {
			idList.add(statusDoc.getId()); // Get Id and add to list
		}
		return idList;
	}
	
	/**
	 * Get count of total redundant data.
	 */
	public void getAllRedundantData(){
		// Get id's from gardenhose collection
		List<Long> generalStreamIdList = getIdFromList(statusDalImpl.getOneHourGeneralStreamData());
		// Get id's from other list
		List<Long> allDataIdList = getIdFromList(statusDalImpl.getAllIdFromCollections());
		allDataIdList.addAll(generalStreamIdList);
		// Create a map of id's as key and with count as value
		HashMap<Long, Integer> idCountMap = new HashMap<>();
		for (Long long1 : allDataIdList) {
			if(idCountMap.containsKey(long1)){
				idCountMap.put(long1, idCountMap.get(long1) + 1); // If id already present increase frequency count
			}else{ // Otherwise add ID
				idCountMap.put(long1, 0);
			}
		}
		
		// Get total count. 
		// Code can be found at : https://stackoverflow.com/questions/1066589/iterate-through-a-hashmap
		int count = 0; // Initialize count
		// Iterate over hashmap
		Iterator<Entry<Long, Integer>> itr = idCountMap.entrySet().iterator();
	    while (itr.hasNext()) {
	        Map.Entry<Long,Integer> pair = (Entry<Long, Integer>)itr.next();
	        count = count + pair.getValue(); // Get total count of redundant data
	        itr.remove(); // to avoid a ConcurrentModificationException
	    }
		
	    System.out.println("Total redundant data:"+count);
	}
	
	/**
	 * Get list of merged tweet documents
	 * @return
	 */
	public List<StatusDoc> getAllMergedTweets(){
		List<StatusDoc> tweets = statusDalImpl.getAllFromMergedDoc();
		return tweets;
	}
	
	/**
	 * Update database with cluster information of tweets
	 * @param clusters
	 */
	public void addClusterInfoToTweets(String clusters){
		int i = 0;
		String[] clusterArray = clusters.split(" "); // Split is cluster information into array
		List<StatusDoc> docs = statusDalImpl.getAllDataMergedCollection();
		for (StatusDoc statusDoc : docs) {
			// As cluster information is in tweet order, simply add cluster data to tweet
			statusDoc.setCluster(Float.parseFloat(clusterArray[i]));
			i++; // increase counter
		}
		statusDalImpl.updateMergedCollection(docs); // Update database with cluster information
	}
	
	/**
	 * Get count of geo-tagged data per cluster
	 * @param clusterCount
	 * @return
	 */
	public HashMap<Float, Integer> getClusterGeoTaggedCount(float clusterCount) {
		List<StatusDoc> docs = statusDalImpl.getGeoAndPlaceDataFromMergedList();// get docs that have geo-information
		// Create map with cluster id as key and count as value
		HashMap<Float, Integer> clusterGeoTaggedCount = new HashMap<>();
		for(float i = 0; i < clusterCount; i++){
			clusterGeoTaggedCount.put(i,0); // Set all clsuter count to 0
		}
		
		for (StatusDoc doc : docs) {
			if (clusterGeoTaggedCount.containsKey(doc.getCluster())) {// If cluster with the same number as map found then inc count. 
				clusterGeoTaggedCount.put(doc.getCluster(), clusterGeoTaggedCount.get(doc.getCluster()) + 1);
			}
		}
		
		return clusterGeoTaggedCount;
	}
	
	/**
	 * Delete all collections related to twitter and not merged.
	 */
	public void deleteAllCollectionData(){
		statusDalImpl.deleteAllFromAllCollections();
	}
	
	/**
	 * Get count of number of tweets for given cluster
	 * @param num
	 */
	public void getClusterCount(float num){
		long count = statusDalImpl.getNumCount(num);
		System.out.println(count);
	}
	
	/**
	 * Get frequency count of each geolocation in each cluster. 
	 * Method is deprecated as it i not used. 
	 */
	@Deprecated
	public void getGeoLocationFreq(int maxClusterNumber) {
		List<StatusDoc> docs = statusDalImpl.getGeoAndPlaceDataFromMergedList(); //Get geo-location details from database
		for (int i = 0; i < maxClusterNumber; i++) {// For each cluster
			int count = 0;// Initialize count
			// Create map where geo-location is key and count is value
			HashMap<GeoLocation, Integer> idCountMap = new HashMap<>();
			for (StatusDoc statusDoc : docs) {
				if (statusDoc.getCluster() == i) {
					if (idCountMap.containsKey(statusDoc.getGeoLocation())) { // If geo-location already in map then increase count
						idCountMap.put(statusDoc.getGeoLocation(), idCountMap.get(statusDoc.getGeoLocation()) + 1);
					} else { // Else add geo-location to map
						idCountMap.put(statusDoc.getGeoLocation(), 0);
					}
				}
			}
			// Get geo-location,count pair that has max frequency from map 
			Map.Entry<GeoLocation, Integer> maxPair = null;
			Iterator<Entry<GeoLocation, Integer>> itr = idCountMap.entrySet().iterator();
			while (itr.hasNext()) {
				Map.Entry<GeoLocation, Integer> pair = (Entry<GeoLocation, Integer>) itr.next();
				if (pair.getValue() > count) {// If count greater than current count
					count = pair.getValue(); // update count and pair
					maxPair = pair;
				}
				itr.remove(); // to avoid a ConcurrentModificationException
			}
			// Print details to console.
			System.out.println("For i = " + i + " Count is:" + count + " and pair details is-");
			if (maxPair != null)
				System.out.println("lat:" + maxPair.getKey().getLatitude() + "long:" + maxPair.getKey().getLongitude()
						+ " count-" + maxPair.getValue());
		}
	}
	
	/**
	 * Return the cluster geo-location when cluster number and max cluster count is given
	 * @param maxClusterNumber
	 * @param cluster
	 * @return
	 */
	public ClusterGeoTag createGlasgowGeoLocationMatrix(int maxClusterNumber, int cluster) {
		double clusterRoot = Math.sqrt(maxClusterNumber); // Get square root of cluster size
		double clusterInterval = Math.pow(clusterRoot, -1); // get division count
		double maxLat = 56, minLong = -5; // latitude and longitude range to start from
		GeoLocation glasgowDefaultGeolocation = new GeoLocation(55.86515, -4.25763); // Glasgow city central geo-location
		// Matrix of grid count. Will keep track of number of tweets found in each grid cell.
		int arr[][] = new int[(int) clusterRoot][(int) clusterRoot];
		List<StatusDoc> docs = statusDalImpl.getGeoAndPlaceDataFromMergedList(); // Get Glasgow geo-tag data from database
		// Iterator of tweets docs.
		Iterator<StatusDoc> itrDoc = docs.iterator();
			double latStart = 0, latStop = 0, longStart = 0, longStop = 0, lat = 0, lon = 0;
			// Initialize array matrix
			for (int i = 0; i < arr.length; i++) {
				for (int j = 0; j < arr[i].length; j++) {
					arr[i][j] = 0;
				}
			}
			// Iterate over tweets
			while (itrDoc.hasNext()) {
				StatusDoc doc = itrDoc.next();
				if (doc.getCluster() == cluster) {// If document cluster matches method cluster argument
					// Check if geo-location is null.
					if(doc.getGeoLocation() != null){
						lat = doc.getGeoLocation().getLatitude(); // get doc latitude
						lon = doc.getGeoLocation().getLongitude(); // get doc longitude
					}
					else if(doc.getPlace().getName().equals("Glasgow")){
						lat = glasgowDefaultGeolocation.getLatitude();
						lon = glasgowDefaultGeolocation.getLongitude();
						
					}
					// For each grid row
					for (int i = 0; i < clusterRoot; i++) {
						latStart = maxLat - i * clusterInterval; // Set lat start range using grid division variable clusterInterval
						latStop = maxLat - (i + 1) * clusterInterval; // Set lat end range using grid division variable clusterInterval
						// For each grid column
						for (int j = 0; j < clusterRoot; j++) {
							longStart = minLong + j * clusterInterval; // Set long start range using grid division variable clusterInterval
							longStop = minLong + (j + 1) * clusterInterval; // Set long end range using grid division variable clusterInterval
							// Check if there is a tweet that falls in this geo-location box coordinate or cell 
							if (latStart >= lat && lat >= latStop && longStart <= lon && lon <= longStop) {
								arr[i][j] = arr[i][j] + 1; // add to the count for that cell
							}
						}
					}
					itrDoc.remove(); // Remove the docs already checked.
				}
			}
			int totalCount = 0, max = 0;
			double newLatStart = 0, newLatStop = 0, newLongStart = 0, newLongStop = 0;
			// Get box coordinate with max frequency of occurrence of tweet.
			for (int i = 0; i < arr.length; i++) {
				for (int j = 0; j < arr[i].length; j++) {
					totalCount = totalCount + arr[i][j]; // Keep count of number Glasgow tweets for that cluster
					if (max < arr[i][j]) {
						max = arr[i][j];
						// Form the box
						newLatStart = maxLat - i * clusterInterval;
						newLatStop = maxLat - (i + 1) * clusterInterval;
						newLongStart = minLong + j * clusterInterval;
						newLongStop = minLong + (j + 1) * clusterInterval;
					}
				}
			}
			// Find the box middle which will be set a cluster geo-location
			double latitude = newLatStart - Math.abs(newLatStart - newLatStop) / 2;
			double longitude = newLongStart + Math.abs(newLongStart - newLongStop) / 2;
			GeoLocation loc = new GeoLocation(latitude, longitude); // create geo-location object with the coordinates
			ClusterGeoTag tag = new ClusterGeoTag(cluster, totalCount, loc); // Create cluster details object.
		return tag;
	}
	
	/**
	 * Similar to createGlasgowGeoLocationMatrix() except that it iterates over only 50% of
	 * the geo-tagged tweets.
	 * @param maxClusterNumber
	 * @param cluster
	 */
	public ClusterGeoTag createGlasgowGeoLocationMatrixWithPartialData(int maxClusterNumber, int cluster){
		double clusterRoot = Math.sqrt(maxClusterNumber); // Get square root of cluster size
		double clusterInterval = Math.pow(clusterRoot, -1); // get division count
		double maxLat = 56, minLong = -5; // latitude and longitude range to start from
		GeoLocation glasgowDefaultGeolocation = new GeoLocation(55.86515, -4.25763); // Glasgow city central geo-location
		// Matrix of grid count. Will keep track of number of tweets found in each grid cell.
		int arr[][] = new int[(int) clusterRoot][(int) clusterRoot];
		List<StatusDoc> docs = statusDalImpl.getHalfGeoAndPlaceDataFromMergedList(); // Get Glasgow geo-tag data from database
		// Iterator of tweets docs.
		Iterator<StatusDoc> itrDoc = docs.iterator();
			double latStart = 0, latStop = 0, longStart = 0, longStop = 0, lat = 0, lon = 0;
			// Initialize array matrix
			for (int i = 0; i < arr.length; i++) {
				for (int j = 0; j < arr[i].length; j++) {
					arr[i][j] = 0;
				}
			}
			// Iterate over tweets
			while (itrDoc.hasNext()) {
				StatusDoc doc = itrDoc.next();
				if (doc.getCluster() == cluster) {// If document cluster matches method cluster argument
					// Check if geo-location is null.
					if(doc.getGeoLocation() != null){
						lat = doc.getGeoLocation().getLatitude(); // get doc latitude
						lon = doc.getGeoLocation().getLongitude(); // get doc longitude
					}
					// If geo-data is null then use Place data in case the place is Glasgow.
					else if(doc.getPlace().getName().equals("Glasgow")){
						lat = glasgowDefaultGeolocation.getLatitude();
						lon = glasgowDefaultGeolocation.getLongitude();
						
					}
					// For each grid row
					for (int i = 0; i < clusterRoot; i++) {
						latStart = maxLat - i * clusterInterval; // Set lat start range using grid division variable clusterInterval
						latStop = maxLat - (i + 1) * clusterInterval; // Set lat end range using grid division variable clusterInterval
						// For each grid column
						for (int j = 0; j < clusterRoot; j++) {
							longStart = minLong + j * clusterInterval; // Set long start range using grid division variable clusterInterval
							longStop = minLong + (j + 1) * clusterInterval; // Set long end range using grid division variable clusterInterval
							// Check if there is a tweet that falls in this geo-location box coordinate or cell 
							if (latStart >= lat && lat >= latStop && longStart <= lon && lon <= longStop) {
								arr[i][j] = arr[i][j] + 1; // add to the count for that cell
							}
						}
					}
					itrDoc.remove(); // Remove the docs already checked.
				}
			}
			int totalCount = 0, max = 0;
			double newLatStart = 0, newLatStop = 0, newLongStart = 0, newLongStop = 0;
			// Get box coordinate with max frequency of occurrence of tweet.
			for (int i = 0; i < arr.length; i++) {
				for (int j = 0; j < arr[i].length; j++) {
					totalCount = totalCount + arr[i][j]; // Keep count of number Glasgow tweets for that cluster
					if (max < arr[i][j]) {
						max = arr[i][j];
						// Form the box
						newLatStart = maxLat - i * clusterInterval;
						newLatStop = maxLat - (i + 1) * clusterInterval;
						newLongStart = minLong + j * clusterInterval;
						newLongStop = minLong + (j + 1) * clusterInterval;
					}
				}
			}
			// Find the box middle which will be set a cluster geo-location
			double latitude = newLatStart - Math.abs(newLatStart - newLatStop) / 2;
			double longitude = newLongStart + Math.abs(newLongStart - newLongStop) / 2;
			GeoLocation loc = new GeoLocation(latitude, longitude); // create geo-location object with the coordinates
			ClusterGeoTag tag = new ClusterGeoTag(cluster, totalCount, loc); // Create cluster details object.
		return tag;
	}
}
