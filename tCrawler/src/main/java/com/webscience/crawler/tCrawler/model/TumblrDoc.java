package com.webscience.crawler.tCrawler.model;

import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.tumblr.jumblr.types.Note;
import com.tumblr.jumblr.types.Post;
import com.tumblr.jumblr.types.Post.PostType;

@Document
public class TumblrDoc {

	protected PostType type;
	@Id
    private Long id;
    private String author;
    private String blog_name;
    private String post_url;
    private Long timestamp;
    private Long liked_timestamp;
    private String state;
    private String date;
    private List<String> tags;
    private Long reblogged_from_id;
    private Long reblogged_root_id;
    private Long note_count;
    private List<Note> notes;
	
	public TumblrDoc() {
		// TODO Auto-generated constructor stub
	}
	
	public TumblrDoc(Post post){
		this.id = post.getId();
		this.author = post.getAuthorId();
		this.blog_name = post.getBlogName();
		this.post_url = post.getPostUrl();
		this.timestamp = post.getTimestamp();
		this.liked_timestamp = post.getLikedTimestamp();
		this.state = post.getState();
		this.date = post.getDateGMT();
		this.tags = post.getTags();
		this.reblogged_from_id = post.getRebloggedFromId();
		this.reblogged_root_id = post.getRebloggedRootId();
		this.note_count = post.getNoteCount();
		this.notes = post.getNotes();
	}

	public PostType getType() {
		return type;
	}

	public void setType(PostType type) {
		this.type = type;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getBlog_name() {
		return blog_name;
	}

	public void setBlog_name(String blog_name) {
		this.blog_name = blog_name;
	}

	public String getPost_url() {
		return post_url;
	}

	public void setPost_url(String post_url) {
		this.post_url = post_url;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Long getLiked_timestamp() {
		return liked_timestamp;
	}

	public void setLiked_timestamp(Long liked_timestamp) {
		this.liked_timestamp = liked_timestamp;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public Long getReblogged_from_id() {
		return reblogged_from_id;
	}

	public void setReblogged_from_id(Long reblogged_from_id) {
		this.reblogged_from_id = reblogged_from_id;
	}

	public Long getReblogged_root_id() {
		return reblogged_root_id;
	}

	public void setReblogged_root_id(Long reblogged_root_id) {
		this.reblogged_root_id = reblogged_root_id;
	}

	public Long getNote_count() {
		return note_count;
	}

	public void setNote_count(Long note_count) {
		this.note_count = note_count;
	}

	public List<Note> getNotes() {
		return notes;
	}

	public void setNotes(List<Note> notes) {
		this.notes = notes;
	}
	
}
