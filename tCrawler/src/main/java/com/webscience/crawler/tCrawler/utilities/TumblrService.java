package com.webscience.crawler.tCrawler.utilities;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.tumblr.jumblr.JumblrClient;
import com.tumblr.jumblr.types.Post;
import com.tumblr.jumblr.types.User;
import com.webscience.crawler.tCrawler.model.TumblrDoc;
import com.webscience.crawler.tCrawler.mongodal.StatusDalImpl;

@Service
public class TumblrService {

	private static final String TUMBLR_CONSUMER_KEY = "";
	private static final String TUMBLR_CONSUMER_SECRET = "";
	private static final String TUMBLR_OUTH_TOKEN = "";
	private static final String TUMBLER_OUTH_TOKEN_SECRET = "";

	private final String[] tagArray = { "books", "lol", "fashion", "art", "DIY", "food", "landscape", "illustration",
			"vintage", "design", "film", "animals", "gif", "selfi", "NASA", "space", "lgbt", "trans", "rock", "love",
			"hate", "girl", "boy", "vote", "planets", "funny", "rdr2", "gaming", "film", "nebula", "design", "televion",
			"cat", "dog", "kitten", "cats", "dogs", "sweet", "disney", "sun", "moon", "anonymous", "ask", "birds",
			"literature", "indie", "halloween", "memes", "quotes", "nature", "travel", "undertale", "hp", "jojo",
			"nintendo", "got7", "got", "omg", "life", "dan and phil", "meme", "people", "corgi", "comics", "twd",
			"face", "writing", "beauty", "makeup", "medic", "srsfunny", "scotland", "coffee", "successful", "haikyuu",
			"photography", "night", "meirl", "robin", "D&D", "batman", "superman", "nightwing", "titans", "midterms",
			"glasgow", "football", "weather", "storm", "brexit", "trump", "winter", "best", "festival", "job", "crazy",
			"dream", "game", "christmas", "player", "marvel", "cold", "uk", "america" };

	private JumblrClient jumblrClient;

	@Autowired
	private StatusDalImpl statusDalImpl;

	public TumblrService() {
		jumblrClient = new JumblrClient(TUMBLR_CONSUMER_KEY, TUMBLR_CONSUMER_SECRET);
		jumblrClient.setToken(TUMBLR_OUTH_TOKEN, TUMBLER_OUTH_TOKEN_SECRET);
	}

	public void getTumblrTagPostDetails() {
		User user = jumblrClient.user();
		System.out.println("Using " + user.getName() + " to get tagged data");

		for (int i = 0; i < tagArray.length; i++) {
			List<Post> taggedPost = jumblrClient.tagged(tagArray[i]);
			System.out.println("Tumblr post data for #" + tagArray[i]);
			statusDalImpl.saveTumblrList(taggedPost);
		}

		System.out.println("All data added to database");
	}

	public void getTotalData() {
		statusDalImpl.countAllTumblrDocs();
	}

	public void getBlogWithMaxNotes() {
		statusDalImpl.getMaxNotesForPost();
	}

	public void getMaxAuthorCount() {
		List<TumblrDoc> docs = statusDalImpl.getMaxOccuredTumblrBlog();
		Map<String, Integer> postMap = new HashMap<>();
		for (TumblrDoc tumblrDoc : docs) {
			String blogName = tumblrDoc.getBlog_name();
			if (postMap.containsKey(blogName)) {
				postMap.put(blogName, postMap.get(blogName) + 1);
			} else {
				postMap.put(blogName, 0);
			}
		}

		// Get max frequency blog.
		// Code can be found at :
		// https://stackoverflow.com/questions/1066589/iterate-through-a-hashmap
		int max = 0;
		String blogName = null;
		Iterator<Entry<String, Integer>> itr = postMap.entrySet().iterator();
		while (itr.hasNext()) {
			Map.Entry<String, Integer> pair = (Entry<String, Integer>) itr.next();
			if(max < pair.getValue()){
				max = pair.getValue();
				blogName = pair.getKey();
			}
			itr.remove(); // to avoid a ConcurrentModificationException
		}

		System.out.println("Blog with max posts in database is:" + blogName + " with "+ max + " posts.");
	}

	public int getDuplicateData() {
		return statusDalImpl.getDuplicateData();
	}

}
