package com.webscience.crawler.tCrawler.mongodal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import com.tumblr.jumblr.types.Post;
import com.webscience.crawler.tCrawler.model.StatusDoc;
import com.webscience.crawler.tCrawler.model.TumblrDoc;

import twitter4j.Status;

/**
 * Repository class and is part of the Data access layer.
 * It is responsible for interacting with MongoDb. 
 * 
 * @author Sheena Gaur
 *
 */
@Repository
public class StatusDalImpl implements StatusDal {
	
	// get MongoTemplate object from Spring Context. To be used to connect to the database. 
	@Autowired
	private MongoTemplate mongoTemplate;
	
	// Constants that represents the collection names.
	private static final String STREAM_COLLECTION = "StreamCollection"; // To store Gardenhose data
	private static final String STREAM_LOCATION_COLLECTION = "StreamLocationCollection"; // To store Stream location based data
	private static final String STREAM_TOPIC_COLLECTION = "StreamTopicCollection";// To store stream topic based data 
	private static final String REST_COLLECTION = "RestCollection"; // To store REST API data
	private static final String MERGED_COLLECTION = "StreamRestMergedCollection"; // To tore merged tweets
	private static final String TUMBLR_COLLECTION = "TumblrCollection"; // To tore tumblr data
	
	// Tumblr duplicate data count
	private int duplicateData = 0;

	/**
	 * Add gardenhose tweet to database
	 */
	@Override
	public Status addStreamStatus(Status status) {
		StatusDoc doc = new StatusDoc(status); // Create StatusDoc object
		mongoTemplate.insert(doc, STREAM_COLLECTION); // Insert to database
		return status;
	}

	/**
	 * Add topic based stream tweet to database.
	 */
	@Override
	public Status addStreamStatusTopicFilter(Status status) {
		StatusDoc doc = new StatusDoc(status); // Create StatusDoc object
		mongoTemplate.insert(doc, STREAM_TOPIC_COLLECTION); // Insert to database
		return status;
	}

	/**
	 * Add location stream tweet to database.
	 */
	@Override
	public Status addStreamStatusLocationFilter(Status status) {
		StatusDoc doc = new StatusDoc(status); // Create StatusDoc object
		mongoTemplate.insert(doc, STREAM_LOCATION_COLLECTION); // Insert to database
		return status;
	}

	/**
	 * Add REST API tweet to database.
	 */
	@Override
	public Status addRestStatusTopicFilter(Status status) {
		StatusDoc doc = new StatusDoc(status); // Create StatusDoc object
		mongoTemplate.insert(doc, REST_COLLECTION); // Insert to database
		return status;
	}
	
	/**
	 * Get total document count.
	 */
	@Override
	public void findAllDocs(){
		Query query = new Query(); // Create query object
		query.addCriteria(Criteria.where("_id").exists(true)); // Add query criteria i.e. if ID exist
		long count = getTotalCount(query); // Get count of documents that match query
		System.out.println("Total Docs:" + count);
	}
	
	/**
	 * Using set theory ,i.e., n(A union B) = n(A) + n(B) - n(A intersection B)
	 * we can find the geo-tagged data.
	 */
	@Override
	public void getCountGeoTagged(){
		long count = getCountGeoOnly() + getCountPlaceOnly() - getCountGeoAndPlace();
		System.out.println("Total Geo Tagged Data:" + count);
	}
	
	/**
	 * Get documents that have both Geo-location and Place data
	 */
	@Override
	public long getCountGeoAndPlace(){
		Query query = new Query();
		query.addCriteria(Criteria.where("geoLocation").exists(true).andOperator(Criteria.where("place").exists(true)));
		long count = getTotalCount(query);
		return count;
	}
	
	/**
	 * Get document that only have geo-data
	 */
	@Override
	public long getCountGeoOnly(){
		Query query = new Query();
		query.addCriteria(Criteria.where("geoLocation").exists(true));
		long count = getTotalCount(query);
		return count;
	}
	
	/**
	 * Get documents that have only Place data
	 */
	@Override
	public long getCountPlaceOnly(){
		Query query = new Query();
		query.addCriteria(Criteria.where("place").exists(true));
		long count = getTotalCount(query);
		return count;
	}
	
	/**
	 * Get count of re-tweets
	 */
	@Override
	public void getRetweetCount(){
		Query query = new Query();
		query.addCriteria(Criteria.where("isRetweeted").is(true)); // If isRetweeted column is true.
		long count = getTotalCount(query);
		System.out.println("Total Retweet Count:" + count);
	}
	
	/**
	 * Get count of quotes
	 */
	@Override
	public void getQuotedCount(){
		Query query = new Query();
		query.addCriteria(Criteria.where("quotedStatusId").ne(-1L)); // If quoteStatusId is not -1 i.e. it is a quote
		long count = getTotalCount(query);
		System.out.println("Total Quoted Count:" + count);
	}
	
	/**
	 * Get total count from each collection of the given query.
	 * @param query
	 * @return
	 */
	private long getTotalCount(Query query){
		long count = mongoTemplate.count(query, REST_COLLECTION);
		count = count + mongoTemplate.count(query, STREAM_LOCATION_COLLECTION);
		count = count + mongoTemplate.count(query, STREAM_TOPIC_COLLECTION);
		count = count + mongoTemplate.count(query, STREAM_COLLECTION);
		return count;
	}
	
	/**
	 * Get ID of the documents that in the Stream Location collection.
	 * They contain Glasgow location tweets 
	 */
	@Override
	public List<StatusDoc> getGlasgowTaggedDocs(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true)); // Where Id exits
		query.fields().include("_id"); // get only Id column
		List<StatusDoc> statuses = mongoTemplate.find(query,StatusDoc.class, STREAM_LOCATION_COLLECTION);
		return statuses;
	}
	
	/**
	 * Get ID's from all the collections as list.
	 */
	@Override
	public List<StatusDoc> getAllIdFromCollections(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		query.fields().include("_id"); // only include ID column
		List<StatusDoc> statuses_loc = mongoTemplate.find(query,StatusDoc.class, STREAM_LOCATION_COLLECTION);
		List<StatusDoc> statuses_topic = mongoTemplate.find(query,StatusDoc.class, STREAM_TOPIC_COLLECTION);
		List<StatusDoc> statuses_rest = mongoTemplate.find(query,StatusDoc.class, REST_COLLECTION);
		
		statuses_loc.addAll(statuses_topic);
		statuses_loc.addAll(statuses_rest);
		return statuses_loc;
	}
	
	/**
	 * Get ID's from collection that has GardenHose data and return as list.
	 */
	@Override
	public List<StatusDoc> getOneHourGeneralStreamData(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		query.fields().include("_id");
		List<StatusDoc> statuses = mongoTemplate.find(query,StatusDoc.class, STREAM_COLLECTION);
		return statuses;
	}
	
	/**
	 * Merge the tweets from all the collections into a single collection.
	 * This will be used for future grouping of tweets. 
	 */
	@Override
	public void mergeAllCollections(){
		// First delete collection if it already exits.
		try{
			Query query = new Query();
			query.addCriteria(Criteria.where("_id").exists(true)); // Where id exist
			query.fields().include("_id"); // Get only ID column
			long count = mongoTemplate.count(query, MERGED_COLLECTION);	// Get count.
			if(count>=0){ // Collection exists.
				System.out.println("Collection already exits. Deleting collection.");
				// Delete collection before re-inserting data.
				mongoTemplate.remove(new Query(), MERGED_COLLECTION);
				System.out.println("Deletion complete");
			}
		}catch(Exception e){
			// Collection does not exist. Proceed with data insertion.
			System.out.println("Collection does not exist, creating new one.");
		}
		
		//Merge documents from collections and remove redundant data
		int countDup = 0;
		List<StatusDoc> docs = mongoTemplate.findAll(StatusDoc.class, STREAM_COLLECTION); // Get data from GardenHose collection		
		Set<StatusDoc> statusSet = new HashSet<>(docs);// Add to Set as set does not allow duplicates
		mongoTemplate.insert(statusSet, MERGED_COLLECTION);// Insert to database
		System.out.println("Documents merged into new Collection:" + MERGED_COLLECTION);
		
		docs = mongoTemplate.findAll(StatusDoc.class, STREAM_LOCATION_COLLECTION); // Get data from Location collection
		statusSet = new HashSet<>(docs);
		// Add to Set and add to database.
				for (StatusDoc statusDoc : docs) {
					try{
						mongoTemplate.insert(statusDoc, MERGED_COLLECTION); //Insert to database
					}catch(DuplicateKeyException ex){
						//System.err.println(ex.getMessage());
						countDup++;
					}
				}
		System.out.println("Documents merged into new Collection:" + MERGED_COLLECTION + " with duplicates:" + countDup);
		countDup = 0;
		docs = mongoTemplate.findAll(StatusDoc.class, STREAM_TOPIC_COLLECTION);// Get data from Topic collection
		statusSet = new HashSet<>(docs);
		// Add to Set and add to database.
		for (StatusDoc statusDoc : docs) {
			try{
				mongoTemplate.insert(statusDoc, MERGED_COLLECTION); //Insert to database
			}catch(DuplicateKeyException ex){
				//System.err.println(ex.getMessage());
				countDup++;
			}
		}
		System.out.println("Documents merged into new Collection:" + MERGED_COLLECTION + " with duplicates:" + countDup);
		
		countDup = 0;
		docs = mongoTemplate.findAll(StatusDoc.class, REST_COLLECTION); // Get data from REST collection
		statusSet = new HashSet<>(docs);
		for (StatusDoc statusDoc : docs) {
			try{
				mongoTemplate.insert(statusDoc, MERGED_COLLECTION); // Add to database
			}catch(DuplicateKeyException ex){
				//System.err.println(ex.getMessage());
				countDup++;
			}
		}
		System.out.println("Documents merged into new Collection:" + MERGED_COLLECTION + " with duplicates:" + countDup);
	}
	
	/**
	 * Get all documents from merged collection. Will return a list containing ID and text from the tweets
	 */
	@Override
	public List<StatusDoc> getAllFromMergedDoc() {
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		query.fields().include("_id").include("text"); // Get only Id and text column
		List<StatusDoc> statuses = mongoTemplate.find(query, StatusDoc.class, MERGED_COLLECTION);
		return statuses;
	}
	
	/**
	 * Get all documents from merged collection. Will return a list of documents.
	 */
	@Override
	public List<StatusDoc> getAllDataMergedCollection(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		List<StatusDoc> statuses = mongoTemplate.find(query, StatusDoc.class, MERGED_COLLECTION);
		return statuses;
	}
	
	/**
	 * Get count of number of tweets per cluster where the cluster number is given as an argument.
	 */
	@Override
	public long getNumCount(float num){
		Query query = new Query();
		query.addCriteria(Criteria.where("cluster").is(num));// Where cluster is the num
		long statuses = mongoTemplate.count(query, MERGED_COLLECTION);
		return statuses;
	}
	
	/**
	 * Update the document in the collection with the list of documents.
	 * @param docs
	 */
	public void updateMergedCollection(List<StatusDoc> docs){
		for (StatusDoc statusDoc : docs) {
			mongoTemplate.save(statusDoc, MERGED_COLLECTION);
		}
		System.out.println("Tweets Updated.");
	}
	
	/**
	 * Save tumblr data to collection.
	 */
	@Override
	public void saveTumblrData(Post post){
		TumblrDoc doc = new TumblrDoc(post);
		try {
			mongoTemplate.insert(doc, TUMBLR_COLLECTION); // Insert to database
		} catch (Exception e) {
			System.out.println("Duplicate Data received...adding to duplicate count");
			duplicateData++; // Keeping count of duplicate data
		}
	}
	
	/**
	 * Add lit of Posts to database. 
	 */
	@Override
	public void saveTumblrList(List<Post> posts){
		for (Post post : posts) {
			saveTumblrData(post);
		}
	}
	
	/**
	 * get count of all tumble documents
	 */
	@Override
	public void countAllTumblrDocs(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true)); // Where ID is present
		long count = mongoTemplate.count(query, TUMBLR_COLLECTION);
		System.out.println("Total Tumbr posts:" + count);
	}
	
	/**
	 * Get the blog post with the most not count 
	 */
	@Override
	public void getMaxNotesForPost(){
		Query query = new Query();
		query.addCriteria(Criteria.where("note_count").exists(true)); // Get notecount
		query.fields().include("note_count").include("blog_name"); // Include note_count and blog_name column
		List<TumblrDoc> notes = mongoTemplate.find(query, TumblrDoc.class, TUMBLR_COLLECTION); // Get data from database
		TumblrDoc maxNoteDoc  = notes.get(0); // Set max to the first data inn list
		for (TumblrDoc tumblrDoc : notes) {
			if(maxNoteDoc.getNote_count() < tumblrDoc.getNote_count()) // Compare note counts
				maxNoteDoc = tumblrDoc;
		}
		System.out.println("Max note count in all the posts is:" + maxNoteDoc.getNote_count() + " and the author is "+ maxNoteDoc.getBlog_name());
	}
	
	/**
	 * Get blog post with max number of occurrences in the database
	 */
	@Override
	public List<TumblrDoc> getMaxOccuredTumblrBlog(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		query.fields().include("blog_name"); // Only get Blog name
		List<TumblrDoc> posts = mongoTemplate.find(query, TumblrDoc.class, TUMBLR_COLLECTION);
		return posts;
	}
	
	/**
	 * Get documents that have geo-location and get documents that 
	 * have place information from merged collection.
	 */
	@Override
	public List<StatusDoc> getGeoAndPlaceDataFromMergedList(){
		Query query = new Query();
		query.addCriteria(Criteria.where("geoLocation").exists(true)); // get data that has geo-location
		List<StatusDoc> docs1 = mongoTemplate.find(query,StatusDoc.class, MERGED_COLLECTION);
		query.addCriteria(Criteria.where("place").exists(true)); // get data that has place
		List<StatusDoc> docs2 = mongoTemplate.find(query,StatusDoc.class, MERGED_COLLECTION);
		Set<StatusDoc> statusSet = new HashSet<>(docs1);
		statusSet.addAll(docs2); // merge the lists
		List<StatusDoc> allGeoTaggedList = new ArrayList<>(statusSet); // get array list from set.
		return allGeoTaggedList;
	}
	
	/**
	 * Get half documents that have geo-location and get documents that 
	 * have place information from merged collection.
	 * @return StatusDoc lists
	 */
	@Override
	public List<StatusDoc> getHalfGeoAndPlaceDataFromMergedList(){
		List<StatusDoc> geoList = getGeoAndPlaceDataFromMergedList(); // Get all geo-location Data
		List<StatusDoc> halfList = new ArrayList<>();
		int len = geoList.size()/2; // Get half the size of list
		for(int i =0; i<len;i++){
			halfList.add(geoList.get(i)); // Add half of the geo-list to new list.
		}
		return halfList;
	}
	
	/**
	 * Delete all the collections that are Twitter related and non-merged
	 */
	@Override
	public void deleteAllFromAllCollections(){
		mongoTemplate.remove(new Query(), STREAM_COLLECTION);
		mongoTemplate.remove(new Query(), STREAM_LOCATION_COLLECTION);
		mongoTemplate.remove(new Query(), STREAM_TOPIC_COLLECTION);
		mongoTemplate.remove(new Query(), REST_COLLECTION);
	}
	
	/**
	 * Get duplicate data count.
	 * @return
	 */
	public int getDuplicateData() {
		return duplicateData;
	}

}
