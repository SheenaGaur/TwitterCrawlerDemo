package com.webscience.crawler.tCrawler.model;

import java.util.Date;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.Place;
import twitter4j.Scopes;
import twitter4j.Status;
import twitter4j.SymbolEntity;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;

@Document
public class StatusDoc {

	private Date createdAt;
	@Id
    private long id;
	
    private String text;
    private int displayTextRangeStart = -1;
    private int displayTextRangeEnd = -1;
    private String source;
    private boolean isTruncated;
    private long inReplyToStatusId;
    private long inReplyToUserId;
    private boolean isRetweeted;
    private String inReplyToScreenName;
    private GeoLocation geoLocation;
    private Place place;
    private long retweetCount;
    private String lang;
    private Status retweetedStatus;
    private UserMentionEntity[] userMentionEntities;
    private URLEntity[] urlEntities;
    private HashtagEntity[] hashtagEntities;
    private MediaEntity[] mediaEntities;
    private SymbolEntity[] symbolEntities;
    private long currentUserRetweetId;
    private Scopes scopes;
    private User user = null;
    private Status quotedStatus;
    private long quotedStatusId = -1L;
    
    public StatusDoc() {
		// TODO Auto-generated constructor stub
	}
    
    public StatusDoc(Status status) {
		this.createdAt = status.getCreatedAt();
		this.id = status.getId();
		this.text = status.getText();
		this.displayTextRangeStart = status.getDisplayTextRangeStart();
		this.displayTextRangeEnd = status.getDisplayTextRangeEnd();
		this.source = status.getSource();
		this.isTruncated = status.isTruncated();
		this.inReplyToStatusId = status.getInReplyToStatusId();
		this.inReplyToUserId = status.getInReplyToUserId();
		this.isRetweeted = status.isRetweet();
		this.inReplyToScreenName = status.getInReplyToScreenName();
		this.geoLocation = status.getGeoLocation();
		this.place = status.getPlace();
		this.retweetCount = status.getRetweetCount();
		this.lang = status.getLang();
		this.retweetedStatus = status.getRetweetedStatus();
		this.userMentionEntities = status.getUserMentionEntities();
		this.urlEntities = status.getURLEntities();
		this.hashtagEntities = status.getHashtagEntities();
		this.mediaEntities = status.getMediaEntities();
		this.symbolEntities = status.getSymbolEntities();
		this.currentUserRetweetId = status.getCurrentUserRetweetId();
		this.scopes = status.getScopes();
		this.quotedStatus = status.getQuotedStatus();
		this.quotedStatusId = status.getQuotedStatusId();
	}
    
	public Date getCreatedAt() {
		return createdAt;
	}
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public int getDisplayTextRangeStart() {
		return displayTextRangeStart;
	}
	public void setDisplayTextRangeStart(int displayTextRangeStart) {
		this.displayTextRangeStart = displayTextRangeStart;
	}
	public int getDisplayTextRangeEnd() {
		return displayTextRangeEnd;
	}
	public void setDisplayTextRangeEnd(int displayTextRangeEnd) {
		this.displayTextRangeEnd = displayTextRangeEnd;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public boolean isTruncated() {
		return isTruncated;
	}
	public void setTruncated(boolean isTruncated) {
		this.isTruncated = isTruncated;
	}
	public long getInReplyToStatusId() {
		return inReplyToStatusId;
	}
	public void setInReplyToStatusId(long inReplyToStatusId) {
		this.inReplyToStatusId = inReplyToStatusId;
	}
	public long getInReplyToUserId() {
		return inReplyToUserId;
	}
	public void setInReplyToUserId(long inReplyToUserId) {
		this.inReplyToUserId = inReplyToUserId;
	}
	public boolean isRetweeted() {
		return isRetweeted;
	}
	public void setRetweeted(boolean isRetweeted) {
		this.isRetweeted = isRetweeted;
	}
	public String getInReplyToScreenName() {
		return inReplyToScreenName;
	}
	public void setInReplyToScreenName(String inReplyToScreenName) {
		this.inReplyToScreenName = inReplyToScreenName;
	}
	public GeoLocation getGeoLocation() {
		return geoLocation;
	}
	public void setGeoLocation(GeoLocation geoLocation) {
		this.geoLocation = geoLocation;
	}
	public Place getPlace() {
		return place;
	}
	public void setPlace(Place place) {
		this.place = place;
	}
	public long getRetweetCount() {
		return retweetCount;
	}
	public void setRetweetCount(long retweetCount) {
		this.retweetCount = retweetCount;
	}
	public String getLang() {
		return lang;
	}
	public void setLang(String lang) {
		this.lang = lang;
	}
	public Status getRetweetedStatus() {
		return retweetedStatus;
	}
	public void setRetweetedStatus(Status retweetedStatus) {
		this.retweetedStatus = retweetedStatus;
	}
	public UserMentionEntity[] getUserMentionEntities() {
		return userMentionEntities;
	}
	public void setUserMentionEntities(UserMentionEntity[] userMentionEntities) {
		this.userMentionEntities = userMentionEntities;
	}
	public URLEntity[] getUrlEntities() {
		return urlEntities;
	}
	public void setUrlEntities(URLEntity[] urlEntities) {
		this.urlEntities = urlEntities;
	}
	public HashtagEntity[] getHashtagEntities() {
		return hashtagEntities;
	}
	public void setHashtagEntities(HashtagEntity[] hashtagEntities) {
		this.hashtagEntities = hashtagEntities;
	}
	public MediaEntity[] getMediaEntities() {
		return mediaEntities;
	}
	public void setMediaEntities(MediaEntity[] mediaEntities) {
		this.mediaEntities = mediaEntities;
	}
	public SymbolEntity[] getSymbolEntities() {
		return symbolEntities;
	}
	public void setSymbolEntities(SymbolEntity[] symbolEntities) {
		this.symbolEntities = symbolEntities;
	}
	public long getCurrentUserRetweetId() {
		return currentUserRetweetId;
	}
	public void setCurrentUserRetweetId(long currentUserRetweetId) {
		this.currentUserRetweetId = currentUserRetweetId;
	}
	public Scopes getScopes() {
		return scopes;
	}
	public void setScopes(Scopes scopes) {
		this.scopes = scopes;
	}
	public User getUser() {
		return user;
	}
	public void setUser(User user) {
		this.user = user;
	}
	public Status getQuotedStatus() {
		return quotedStatus;
	}
	public void setQuotedStatus(Status quotedStatus) {
		this.quotedStatus = quotedStatus;
	}
	public long getQuotedStatusId() {
		return quotedStatusId;
	}
	public void setQuotedStatusId(long quotedStatusId) {
		this.quotedStatusId = quotedStatusId;
	}
    
}
