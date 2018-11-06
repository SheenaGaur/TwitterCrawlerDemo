package com.webscience.crawler.tCrawler.mongodal;

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
import com.webscience.crawler.tCrawler.utilities.TumblrService;

import twitter4j.Place;
import twitter4j.Status;


@Repository
public class StatusDalImpl implements StatusDal {
	
	@Autowired
	private MongoTemplate mongoTemplate;
	
	private static final String STREAM_COLLECTION = "StreamCollection2";
	private static final String STREAM_LOCATION_COLLECTION = "StreamLocationCollection";
	private static final String STREAM_TOPIC_COLLECTION = "StreamTopicCollection";
	private static final String REST_COLLECTION = "RestCollection";
	private static final String MERGED_COLLECTION = "StreamRestMergedCollection";
	private static final String TUMBLR_COLLECTION = "TumblrCollection";
	
	private int duplicateData = 0;

	@Override
	public Status addStreamStatus(Status status) {
		StatusDoc doc = new StatusDoc(status);
		mongoTemplate.insert(doc, STREAM_COLLECTION);
		return status;
	}

	@Override
	public Status addStreamStatusTopicFilter(Status status) {
		StatusDoc doc = new StatusDoc(status);
		mongoTemplate.insert(doc, STREAM_TOPIC_COLLECTION);
		return status;
	}

	@Override
	public Status addStreamStatusLocationFilter(Status status) {
		StatusDoc doc = new StatusDoc(status);
		mongoTemplate.insert(doc, STREAM_LOCATION_COLLECTION);
		return status;
	}

	@Override
	public Status addRestStatusTopicFilter(Status status) {
		StatusDoc doc = new StatusDoc(status);
		mongoTemplate.insert(doc, REST_COLLECTION);
		return status;
	}
	
	@Override
	public void findAllDocs(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		long count = getTotalCount(query);
		System.out.println("Total Docs:" + count);
	}
	
	/**
	 * Using set theory ,i.e., n(A union B) = n(A) + n(B) - n(A intersection B)
	 * we can find the geo tagged data.
	 */
	@Override
	public void getCountGeoTagged(){
		long count = getCountGeoOnly() + getCountPlaceOnly() - getCountGeoAndPlace();
		System.out.println("Total Geo Tagged Data:" + count);
	}
	
	@Override
	public long getCountGeoAndPlace(){
		Query query = new Query();
		query.addCriteria(Criteria.where("geoLocation").exists(true).andOperator(Criteria.where("place").exists(true)));
		long count = getTotalCount(query);
		return count;
	}
	
	@Override
	public long getCountGeoOnly(){
		Query query = new Query();
		query.addCriteria(Criteria.where("geoLocation").exists(true));
		long count = getTotalCount(query);
		return count;
	}
	
	@Override
	public long getCountPlaceOnly(){
		Query query = new Query();
		query.addCriteria(Criteria.where("place").exists(true));
		long count = getTotalCount(query);
		return count;
	}
	
	@Override
	public void getRetweetCount(){
		Query query = new Query();
		query.addCriteria(Criteria.where("isRetweeted").is(true));
		long count = getTotalCount(query);
		System.out.println("Total Retweet Count:" + count);
	}
	
	@Override
	public void getQuotedCount(){
		Query query = new Query();
		query.addCriteria(Criteria.where("quotedStatusId").ne(-1L));
		long count = getTotalCount(query);
		System.out.println("Total Quoted Count:" + count);
	}
	
	private long getTotalCount(Query query){
		long count = mongoTemplate.count(query, REST_COLLECTION);
		count = count + mongoTemplate.count(query, STREAM_LOCATION_COLLECTION);
		count = count + mongoTemplate.count(query, STREAM_TOPIC_COLLECTION);
		count = count + mongoTemplate.count(query, STREAM_COLLECTION);
		return count;
	}
	
	@Override
	public List<StatusDoc> getGlasgowTaggedDocs(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		query.fields().include("_id");
		List<StatusDoc> statuses = mongoTemplate.find(query,StatusDoc.class, STREAM_LOCATION_COLLECTION);
		return statuses;
	}
	
	@Override
	public List<StatusDoc> getAllIdFromCollections(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		query.fields().include("_id");
		List<StatusDoc> statuses_loc = mongoTemplate.find(query,StatusDoc.class, STREAM_LOCATION_COLLECTION);
		List<StatusDoc> statuses_topic = mongoTemplate.find(query,StatusDoc.class, STREAM_TOPIC_COLLECTION);
		List<StatusDoc> statuses_rest = mongoTemplate.find(query,StatusDoc.class, REST_COLLECTION);
		
		statuses_loc.addAll(statuses_topic);
		statuses_loc.addAll(statuses_rest);
		return statuses_loc;
	}
	
	@Override
	public List<StatusDoc> getOneHourGeneralStreamData(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		query.fields().include("_id");
		List<StatusDoc> statuses = mongoTemplate.find(query,StatusDoc.class, STREAM_COLLECTION);
		return statuses;
	}
	
	@Override
	public void mergeAllCollections(){
		int countDup = 0;
		List<StatusDoc> docs = mongoTemplate.findAll(StatusDoc.class, STREAM_COLLECTION);		
		Set<StatusDoc> statusSet = new HashSet<>(docs);
		mongoTemplate.insert(statusSet, MERGED_COLLECTION);
		System.out.println("Documents merged into new Collection:" + MERGED_COLLECTION);
		
		docs = mongoTemplate.findAll(StatusDoc.class, STREAM_LOCATION_COLLECTION);
		statusSet = new HashSet<>(docs);
		mongoTemplate.insert(statusSet, MERGED_COLLECTION);
		System.out.println("Documents merged into new Collection:" + MERGED_COLLECTION);
		
		docs = mongoTemplate.findAll(StatusDoc.class, STREAM_TOPIC_COLLECTION);
		statusSet = new HashSet<>(docs);
		
		for (StatusDoc statusDoc : docs) {
			try{
				mongoTemplate.insert(statusDoc, MERGED_COLLECTION);
			}catch(DuplicateKeyException ex){
				System.err.println(ex.getMessage());
				countDup++;
			}
		}
		System.out.println("Documents merged into new Collection:" + MERGED_COLLECTION + " with duplicates:" + countDup);
		
		countDup = 0;
		docs = mongoTemplate.findAll(StatusDoc.class, REST_COLLECTION);
		statusSet = new HashSet<>(docs);
		for (StatusDoc statusDoc : docs) {
			try{
				mongoTemplate.insert(statusDoc, MERGED_COLLECTION);
			}catch(DuplicateKeyException ex){
				System.err.println(ex.getMessage());
				countDup++;
			}
		}
		System.out.println("Documents merged into new Collection:" + MERGED_COLLECTION + " with duplicates:" + countDup);
	}
	
	@Override
	public List<StatusDoc> getAllFromMergedDoc() {
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		query.fields().include("_id").include("text");
		List<StatusDoc> statuses = mongoTemplate.find(query, StatusDoc.class, MERGED_COLLECTION);
		return statuses;
	}
	
	@Override
	public void saveTumblrData(Post post){
		TumblrDoc doc = new TumblrDoc(post);
		try {
			mongoTemplate.insert(doc, TUMBLR_COLLECTION);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Duplicate Data received...adding to duplicate count");
			duplicateData++;
		}
	}
	
	@Override
	public void saveTumblrList(List<Post> posts){
		for (Post post : posts) {
			saveTumblrData(post);
		}
	}
	
	@Override
	public void countAllTumblrDocs(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		long count = mongoTemplate.count(query, TUMBLR_COLLECTION);
		System.out.println("Total Tumbr posts:" + count);
	}
	
	@Override
	public void getMaxNotesForPost(){
		Query query = new Query();
		query.addCriteria(Criteria.where("note_count").exists(true));
		query.fields().include("note_count").include("blog_name");
		List<TumblrDoc> notes = mongoTemplate.find(query, TumblrDoc.class, TUMBLR_COLLECTION);
		TumblrDoc maxNoteDoc  = notes.get(0);
		for (TumblrDoc tumblrDoc : notes) {
			if(maxNoteDoc.getNote_count() < tumblrDoc.getNote_count())
				maxNoteDoc = tumblrDoc;
		}
		System.out.println("Max note count in all the posts is:" + maxNoteDoc.getNote_count() + " and the author is "+ maxNoteDoc.getBlog_name());
	}
	
	@Override
	public List<TumblrDoc> getMaxOccuredTumblrBlog(){
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").exists(true));
		query.fields().include("blog_name");
		List<TumblrDoc> posts = mongoTemplate.find(query, TumblrDoc.class, TUMBLR_COLLECTION);
		return posts;
	}
	
	public int getDuplicateData() {
		return duplicateData;
	}

}