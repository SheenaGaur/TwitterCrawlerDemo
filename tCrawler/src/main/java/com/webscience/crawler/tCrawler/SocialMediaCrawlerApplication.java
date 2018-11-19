package com.webscience.crawler.tCrawler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.webscience.crawler.tCrawler.model.ClusterGeoTag;
import com.webscience.crawler.tCrawler.model.StatusDoc;
import com.webscience.crawler.tCrawler.utilities.*;

import twitter4j.Place;
import twitter4j.Status;
import twitter4j.TwitterException;

/**
 * Application main class. Execution begins from this call and utility methods
 * also called.
 * 
 * @author Sheena Gaur
 *
 */
@SpringBootApplication
@EnableScheduling
public class SocialMediaCrawlerApplication {

	/**
	 * Main method. Program execution begins from this method.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// Create application context object. Using this object all other
		// objects are created.
		ApplicationContext context = SpringApplication.run(SocialMediaCrawlerApplication.class, args);
		// Create StreamService Object. Calls Streaming API.
		StreamService streamService = context.getBean(StreamService.class);
		// Create RestService Object. Calls REST API.
		RestService restService = context.getBean(RestService.class);
		// Create TweetStatusService Object. Calls methods to manipulate tweet
		// objects.
		TweetStatusService tweetStatusService = context.getBean(TweetStatusService.class);
		// Create TumblrService Object. Calls Tumblr API.
		TumblrService tumblrService = context.getBean(TumblrService.class);
		// Create ConvertTweetToVec Object. Methods to convert tweet to vector.
		ConvertTweetToVec convertTweetToVec = context.getBean(ConvertTweetToVec.class);

		// try block in case command line arguments are not given.
		try {
			// Get first command line argument.
			String arg = args[0];
			if (arg.equals("1")) {
				System.out.println("\n*******************************************************\n");
				System.out.println("Starting stream and rest calls. These will run for 5 minutes by default.");
				// To begin streaming tweets and populate the database.
				makeStreamAndRestCalls(streamService, restService);
				System.out.println("It is recommended to run the jar file with option 2 so that the new data is merged into the database as well and the data analytics occurs on the new data.");
				System.out.println("\n*******************************************************\n");
			} else if (arg.equals("2")) {
				// Merge all document received to provide data for clustering
				// algorithm. This will merge data to single collection in
				// database.
				System.out.println("\n*******************************************************\n");
				System.out.println("Calling method to merge data of all collections...");
				restService.mergeAllCollectionData();
				System.out.println("\n*******************************************************\n");
			} else if (arg.equals("3")) {
				System.out.println("\n*******************************************************\n");
				// To get counts of data.
				performDataCount(tweetStatusService);
				System.out.println("\n*******************************************************\n");
				// Convert Tweets to vectors
				System.out.println("Pre-processing tweets for vectorisation...");
				// Pre-process document by removing unneeded text data.
				convertTweetToVec.preProcessDocuments();
				System.out.println("Pre-processing done!");
				// Call to conversion function.
				long[][] vector = convertTweetToVec.convertToVectorArray();
				System.out.println(
						"Tweets converted to ineger vector of length:" + vector.length + " by " + vector[0].length);
				System.out.println("Writing vector to file to be read by Python clustering algorithm..");
				// Write converted vector to file. To be used by python.
				writeVectorToCSV(vector);
				System.out.println("Calling Python file for execution.");
				// Get Cluster results from file generated by python and add to
				// database
				String clusters = executePythonClusteringFile();
				if (null == clusters) {
					System.out.println(
							"The cluster creation operation took longer than 2 minutes so timed out. Further processing cannot be done.\n As an alternative, user can manually run the clusterTweets.py file to create clusters and run this file again.");
				} else {
					System.out.println("\n*******************************************************\n");
					// Get max cluster number from file.
					float clusterCount = getClusterCount() + 1.0F; // One added
																	// as
																	// max value
																	// is
																	// one less
																	// than
																	// actual
																	// value.
					// Adding grouping data to tweets in database.
					System.out.println(
							"Adding grouping information to tweets and updating the database. Number of groupings formed:"
									+ clusterCount);
					tweetStatusService.addClusterInfoToTweets(clusters);
					System.out.println("\n*******************************************************\n");
					// Getting geo-tagged cluster information.
					System.out.println(
							"Getting geo-tagged data count per cluster and writing to file.\n The graph information can be found by running tweetgraphNotebook.ipynb file.");
					HashMap<Float, Integer> map = tweetStatusService.getClusterGeoTaggedCount(clusterCount);
					// Write geo-tagged cluster data to file.
					writeClusterCountToFile(map);
					System.out.println("\n*******************************************************\n");
					// Assign geo-location to clusters.
					System.out.println("Generating cluster geo-tagging data with Glasgow as grid location.");
					List<ClusterGeoTag> tags = new ArrayList<>();
					// Do for each cluster
					for (int i = 0; i < clusterCount; i++) {
						// Create a ClusterGeoTag object which has geo-location
						// of
						// cluster and add to list.
						ClusterGeoTag tag = tweetStatusService.createGlasgowGeoLocationMatrix((int) clusterCount, i);
						tags.add(tag);
					}
					// Write this information to console.
					for (ClusterGeoTag clusterGeoTag : tags) {
						System.out.println("Geolocation of cluster: " + clusterGeoTag.getCluster() + " is: " + "(lat= "
								+ clusterGeoTag.getSelectedLocation().getLatitude() + ", long= "
								+ clusterGeoTag.getSelectedLocation().getLongitude() + " )");
						// If no information is present then the cluster did not
						// have Glasgow geo-information in any tweet.
						if (clusterGeoTag.getSelectedLocation().getLatitude() == 0.0) {
							System.out.println("As no location for Cluster-" + clusterGeoTag.getCluster()
									+ " lies in the Glasgow boundaries hence " + "geo-location is unavailable.");
						}
					}
					// Write this information to file to be used to create a
					// graph.
					System.out.println("Write Glasgow geo-tagging information to file");
					writeClusterGlassgowGeoTagInfo(tags);
					System.out.println("\n*******************************************************\n");
					List<ClusterGeoTag> tagsPartial = new ArrayList<>();
					System.out.println("Generating cluster geo-tagging data using only 50% of all geo-tagged data with Glasgow as grid location.");
					for (int i = 0; i < clusterCount; i++) {
						// Create a ClusterGeoTag object which has geo-location
						// of
						// cluster and add to list.
						ClusterGeoTag tag = tweetStatusService.createGlasgowGeoLocationMatrixWithPartialData((int) clusterCount, i);
						tagsPartial.add(tag);
					}
					// Write this information to console.
					for (ClusterGeoTag clusterGeoTag : tagsPartial) {
						System.out.println("Geolocation of cluster: " + clusterGeoTag.getCluster() + " is: " + "(lat= "
								+ clusterGeoTag.getSelectedLocation().getLatitude() + ", long= "
								+ clusterGeoTag.getSelectedLocation().getLongitude() + " )");
						// If no information is present then the cluster did not
						// have Glasgow geo-information in any tweet.
						if (clusterGeoTag.getSelectedLocation().getLatitude() == 0.0) {
							System.out.println("As no location for Cluster-" + clusterGeoTag.getCluster()
									+ " lies in the Glasgow boundaries hence " + "geo-location is unavailable.");
						}
					}
					// Write this information to file to be used to create a
					// graph.
					System.out.println("Write Glasgow partial geo-tagging information to file");
					writeClusterGlassgowGeoTagPartialInfo(tagsPartial);
					System.out.println("\n*******************************************************\n");
				}
				System.out.println("Tumblr data counts:-");
				// Get data counts
				getTumblrDataCounts(tumblrService);
				System.out.println("\n*******************************************************\n");
			} else if (arg.equals("4")) {
				System.out.println("\n*******************************************************\n");
				System.out.println("Getting tumblr posts and adding to database...");
				// Populate database with Tumblr posts
				tumblrService.getTumblrTagPostDetails();
				
				System.out.println("Writing duplicate data to temp...");
				writeDuplicateDataToTemp(tumblrService.getDuplicateData());
				System.out.println("Tumblr Data counts:");
				// Get data counts
				getTumblrDataCounts(tumblrService);
				System.out.println("\n*******************************************************\n");
			}
		} catch (ArrayIndexOutOfBoundsException ex) {
			// If no command line argument then show error and exit.
			System.out.println("No Command line arguments given. Exiting program...");
			System.exit(1);
		}
		// Exit from program.
		System.exit(0);
	}

	/**
	 * Count the amount of tweets collect by calling the database.
	 * 
	 * @param tweetStatusService
	 */
	static void performDataCount(TweetStatusService tweetStatusService) {
		// Total number of documents.
		tweetStatusService.getDocCount();
		// Total Geo-tagged data
		tweetStatusService.getGeoTaggedCount();
		// Get total tweets and quotes.
		tweetStatusService.getRetweetsAndQuotesCount();

		try {
			// Get glasgow geotagged data and find its overlap with streaming
			// data.
			List<StatusDoc> geoTaggedList = tweetStatusService.getGlasgowTaggedDocs();
			tweetStatusService.getOverlapWithGeneralStream(geoTaggedList);
		} catch (Exception e) {
			// Throw exception if data not found.
			e.printStackTrace();
			// Exit from application
			System.exit(1);
		}
		// Get count of redundant data.
		tweetStatusService.getAllRedundantData();
	}

	/**
	 * It makes a call to the Stream API and REST API.
	 * 
	 * @param streamService
	 * @param restService
	 */
	static void makeStreamAndRestCalls(StreamService streamService, RestService restService) {
		// 1. Stream Data without filter for one hour.
		Set<Status> status = streamService.streamApi();

		// Rest calls
		try {
			// 2. Make a REST call to get current trends in Glasgow.
			String trends[] = restService.populateTrends();
			String[] topics = { "glasgow", "football", "weather", "storm", "brexit" };
			String[] filterKeywords = combineStringArrays(topics, trends);
			// 2.1. Stream data using current trends and geo-location of
			// Glasgow.
			streamService.streamTopicApi(filterKeywords); // Topic based
			streamService.streamLocationApi(); // Location based.

			// 3. REST calls to search for given keywords.
			String[] currentTrends = restService.getStr();
			// Increase the search keywords to 56 to gets searches for 1 hr.
			// Some of these
			// keywords may match the trends.
			String[] searchWords = { "glasgow", "football", "weather", "storm", "brexit", "love", "hate", "trump",
					"winter", "firework", "lol", "best", "festival", "job", "rain", "crazy", "dream", "game", "fire",
					"venom", "christmas", "player", "marvel", "batman", "cold", "tweet", "britain", "words" };
			// Combine the strings into a single array.
			String[] result = combineStringArrays(currentTrends, searchWords);
			// Set the topic filter string.
			restService.setStr(result);
			// REST API call
			Set<Status> tweets = restService.getAllTweets();

		} catch (TwitterException e) {
			// Show exception.
			e.printStackTrace();
		}
	}

	/**
	 * Combine the two string arrays into a single array.
	 * 
	 * @param ar1
	 * @param ar2
	 * @return
	 */
	static String[] combineStringArrays(String[] ar1, String[] ar2) {
		// Create an arraylist of first array
		ArrayList<String> list1 = new ArrayList<>(Arrays.asList(ar1));
		// Create an arraylist of second array
		ArrayList<String> list2 = new ArrayList<>(Arrays.asList(ar2));
		// Combine them
		list2.addAll(list1);
		// Convert to array
		String[] result = list2.toArray(new String[0]);
		// Return result.
		return result;
	}

	/**
	 * Write all the tweets received to a text file. Method is not called.
	 * 
	 * @param tweetStatusService
	 * @throws IOException
	 */
	static void writeTweetsToCSV(TweetStatusService tweetStatusService) throws IOException {
		List<StatusDoc> tweets = tweetStatusService.getAllMergedTweets(); // Get
																			// all
																			// tweets.
		System.out.println("Data received from database.");
		File file = new File("./localityprocessing/tweets.txt");// Create a new
																// file
		BufferedWriter writer = new BufferedWriter(new FileWriter(file)); // Create
																			// writer
																			// object.
		// Write to file.
		for (StatusDoc statusDoc : tweets) {
			writer.write(statusDoc.getText());
			writer.newLine();
		}
		System.out.println("Written to file.");
		writer.close(); // Close writer stream
	}

	/**
	 * Write the vector array created to csv file.
	 * 
	 * @param vector
	 * @throws IOException
	 */
	static void writeVectorToCSV(long[][] vector) throws IOException {
		File file = new File("./localityprocessing/vectors.csv"); // Create File
																	// object.
		BufferedWriter writer = new BufferedWriter(new FileWriter(file)); // Create
																			// writer
																			// stream
																			// object.
		// Iterate over vector and write to file.
		for (int i = 0; i < vector.length; i++) {
			for (int j = 0; j < vector[i].length; j++) {
				writer.write(vector[i][j] + "\t");
			}
			writer.newLine();
		}
		System.out.println("Written to file.");
		writer.close(); // Close stream.
	}

	/**
	 * Write the amount of duplicate data collected while streaming tumblr to
	 * file. To be used to display the duplicate data count to the user.
	 * 
	 * @param duplicateData
	 * @throws IOException
	 */
	static void writeDuplicateDataToTemp(int duplicateData) throws IOException {
		File file = new File("./tempData/PostsNumber.txt"); // Create file
															// object.
		BufferedWriter writer = new BufferedWriter(new FileWriter(file)); // Create
																			// write
																			// stream
																			// object.
		writer.write(String.valueOf(duplicateData)); // Write to file.
		writer.newLine();
		System.out.println("Written to file.");
		writer.close(); // Close stream.
	}

	/**
	 * Write to the console the counts of the data analytics. Read the duplicate
	 * data count from file and display to the user. The data is written to file
	 * to preserve this data. Duplicate data is collected when the stream is run
	 * and is lost after.
	 * 
	 * @param tumblrService
	 * @throws IOException
	 */
	static void getTumblrDataCounts(TumblrService tumblrService) throws IOException {
		tumblrService.getTotalData(); // Get total data
		tumblrService.getMaxAuthorCount(); // Get blog with max occurrence.
		tumblrService.getBlogWithMaxNotes(); // Get blog with most notes.
		File file = new File("./tempData/PostsNumber.txt"); // Create file
															// object
		BufferedReader reader = new BufferedReader(new FileReader(file)); // Create
																			// read
																			// stream
																			// object.
		String duplicateData = reader.readLine(); // Read duplicate data count.
		System.out.println("Duplicate data count found while getting Tumblr posts:" + duplicateData);
		reader.close(); // Close stream.
	}

	/**
	 * Read the cluster information from file generated by python.
	 * 
	 * @return
	 * @throws IOException
	 */
	static String readTweetClusterInfo() throws IOException {
		File file = new File("./localityprocessing/centroid_label.txt"); // Create
																			// file
																			// Object.
		BufferedReader reader = new BufferedReader(new FileReader(file)); // Create
																			// reader
																			// object
		String clusters = reader.readLine(); // Read from file and add to string
												// object.
		reader.close(); // Close stream.
		return clusters; // Return string.
	}

	/**
	 * Write the cluster geo-tagged information to a file to be used to create a
	 * graph by python.
	 * 
	 * @param map
	 * @throws IOException
	 */
	static void writeClusterCountToFile(HashMap<Float, Integer> map) throws IOException {
		File file = new File("./localityprocessing/GeoClusterCount.csv"); // Create
																			// file
																			// object.
		BufferedWriter writer = new BufferedWriter(new FileWriter(file)); // Create
																			// writer
																			// object.
		// Iterate over HashMap
		Iterator<Entry<Float, Integer>> itr = map.entrySet().iterator();
		while (itr.hasNext()) { // If map is not empty.
			Map.Entry<Float, Integer> pair = (Entry<Float, Integer>) itr.next();// Get
																				// entry.
			writer.write(pair.getKey() + "\t" + pair.getValue()); // Write to
																	// file the
																	// cluster
																	// name and
																	// count
			writer.newLine();
		}
		System.out.println("Written to file.");
		writer.close(); // CLose stream.
	}

	/**
	 * Method to return the max cluster number from file generated by python.
	 * 
	 * @return size
	 */
	static float getClusterCount() {
		float maxClusterSize = 0; // Initialise size object
		try {
			File file = new File("./localityprocessing/labels_uq.txt"); // Create
																		// file
																		// object
			BufferedReader reader = new BufferedReader(new FileReader(file)); // Create
																				// reader
																				// object
			String clusters = reader.readLine(); // Read from file and add to
													// string.
			String[] clusterLabelsArray = clusters.split(" "); // Split string
																// to get all
																// cluster
																// numbers
			int len = clusterLabelsArray.length;
			maxClusterSize = Float.parseFloat(clusterLabelsArray[len - 1]); // The
																			// last
																			// one
																			// is
																			// amx
			reader.close(); // Close stream
		} catch (IOException e) {
			// Print exception
			e.printStackTrace();
		}
		// Return max size.
		return maxClusterSize;
	}

	/**
	 * The method will execute python file that creates clusters and then read
	 * that cluster information from the file that is written to by python.
	 * 
	 * @return clusters String
	 */
	static String executePythonClusteringFile() {
		// Initialize variables
		String clusters = null;
		int count = 0;
		try {
			// Code taken from
			// https://bytes.com/topic/python/insights/949995-three-ways-run-python-programs-java
			ProcessBuilder pb = new ProcessBuilder("python", "clusterTweets.py"); // Create
																					// a
																					// python
																					// process
			Process p = pb.start(); // Execute that process
			File file = new File("./localityprocessing/centroid_label.txt");// Create
																			// new
																			// file
																			// object
			System.out.print("Checking for grouping completion (max time 2 min)...");
			while (true) { // Loop until file is created.
				System.out.print(".");
				if (file.exists() == true) { // Check if file has been created
												// by python yet
					System.out.println("\nGroupings created! Data written to centroid_label.txt file");
					clusters = readTweetClusterInfo(); // Read cluter
														// infromation and write
														// to string.
					break;
				} else if (count == 120) { // Break loop if program takes more
											// than 100 seconds
					break;
				}
				Thread.sleep(1000); // Sleep for 1 sec
				count++; // Increment count.
			}

		} catch (Exception e) {
			// Print exception
			System.out.println(e);
		}
		// Return clusters
		return clusters;
	}

	/**
	 * Write Glasgow geo-tagged cluster information to file to be used to create
	 * a graph.
	 * 
	 * @param tags
	 * @throws IOException
	 */
	static void writeClusterGlassgowGeoTagInfo(List<ClusterGeoTag> tags) throws IOException {
		File file = new File("./localityprocessing/GlagowGeoClusterCount.csv"); // Create
																				// file
																				// object.
		BufferedWriter writer = new BufferedWriter(new FileWriter(file)); // Create
																			// write
																			// stream
																			// object.
		// Iterate over cluster information
		Iterator<ClusterGeoTag> itr = tags.iterator();
		while (itr.hasNext()) {
			ClusterGeoTag geotag = itr.next();
			// Write tot filethe cluster name, Glasgow count, latitude,
			// longitude
			writer.write(geotag.getCluster() + "\t" + geotag.getTotalGlasgowTagged() + "\t"
					+ geotag.getSelectedLocation().getLatitude() + "" + geotag.getSelectedLocation().getLongitude());
			writer.newLine();
		}
		System.out.println("Written to file.");
		writer.close(); // Close tream.
	}
	
	/**
	 * Write Glasgow geo-tagged cluster information to file to be used to create
	 * a graph.
	 * 
	 * @param tags
	 * @throws IOException
	 */
	static void writeClusterGlassgowGeoTagPartialInfo(List<ClusterGeoTag> tags) throws IOException {
		File file = new File("./localityprocessing/GlagowPartialGeoClusterCount.csv"); // Create
																				// file
																				// object.
		BufferedWriter writer = new BufferedWriter(new FileWriter(file)); // Create
																			// write
																			// stream
																			// object.
		// Iterate over cluster information
		Iterator<ClusterGeoTag> itr = tags.iterator();
		while (itr.hasNext()) {
			ClusterGeoTag geotag = itr.next();
			// Write tot filethe cluster name, Glasgow count, latitude,
			// longitude
			writer.write(geotag.getCluster() + "\t" + geotag.getTotalGlasgowTagged() + "\t"
					+ geotag.getSelectedLocation().getLatitude() + "" + geotag.getSelectedLocation().getLongitude());
			writer.newLine();
		}
		System.out.println("Written to file.");
		writer.close(); // Close tream.
	}
}
