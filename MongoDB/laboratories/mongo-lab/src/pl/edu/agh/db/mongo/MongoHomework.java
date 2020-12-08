package pl.edu.agh.db.mongo;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoHomework {

	private MongoClient mongoClient;
	private MongoDatabase mdb;
	private DB db;
	
	private static long startTime, endTime;
	
	public MongoHomework() throws UnknownHostException{
		
		mongoClient = new MongoClient();
		db = mongoClient.getDB("BartoszKordek2");
		mdb = mongoClient.getDatabase("BartoszKordek2");
		
	}
	
	private void showCollections() {
		for(String name : db.getCollectionNames()) {
			System.out.println("collection: " + name);
		}
	}
	
	private AggregateIterable<Document> businessCities() {
		
		Document city = new Document("city", "$city");
		Document groupField = new Document("_id", city);
		Document group = new Document("$group", groupField);
		Document sort = new Document("$sort", new Document("_id", 1));
		
		MongoCollection<Document> collection = mdb.getCollection("business");
		List<Document> pipeline = Arrays.asList(group, sort);
		return collection.aggregate(pipeline);
		
	}
	
	private int moviesAfter2011() {
		int counter = 0;
		Document gte2011 = new Document("$gte", "2011-01-01");
		Document date = new Document("date", gte2011);
		Document match = new Document("$match", date);
		
		MongoCollection<Document> collection = mdb.getCollection("review");
		List<Document> pipeline = Arrays.asList(match);	
		AggregateIterable<Document> results  = collection.aggregate(pipeline);
		
		for(Document d : results) counter++;
	
		return counter;
	}
	
	private int  moviesAfter2011BetterSolution() {
		Document gte2011 = new Document("$gte", "2011-01-01");
		DBObject query = new BasicDBObject();
		query.put("date", gte2011);
		DBCollection gettedCollection = db.getCollection("review"); 
		return gettedCollection.find(query).count();
	}
	
	
	private AggregateIterable<Document> getClosedBusinesses(){
		
		Document match = new Document("$match", new Document("open", true));
		Document elements = new Document("name", "$name");
		elements.put("address", "$full_address");
		elements.put("stars", "$stars");
		Document project = new Document("$project",  elements);

		MongoCollection<Document> collection = mdb.getCollection("business");
		List<Document> pipeline = Arrays.asList(match, project);
		return collection.aggregate(pipeline);
	}
	
	private AggregateIterable<Document> getNoFunnyAndUsefulUsers(){
		
		Document votesFunny = new Document("votes.funny", 0);
		Document votesUseful = new Document("votes.useful", 0);
		Document match = new Document("$match", new Document("$and", Arrays.asList(votesFunny,votesUseful)));
		Document sort = new Document("$sort", new Document("name", 1));
		
		MongoCollection<Document> collection = mdb.getCollection("user");
		List<Document> pipeline = Arrays.asList(match, sort);
		return collection.aggregate(pipeline);
	}
	
	private AggregateIterable<Document> getBusinessTips(){
		
		Document dateGreaterThan = new Document("date", new Document("$gte", "2012-01-01"));
		Document dateLessThan = new Document("date", new Document("$lte","2012-12-31"));
		Document match = new Document("$match", new Document("$and", Arrays.asList(dateGreaterThan, dateLessThan)));
		
		Document businessTotal = new Document("_id", new Document("business", "$business_id")); 
		businessTotal.put("total", new Document("$sum", 1));
		Document group = new Document("$group", businessTotal);
		Document sort = new Document("$sort", new Document("total", -1));
		
		List<Document> pipeline = Arrays.asList(match, group, sort);
		MongoCollection<Document> collection = mdb.getCollection("tip");
		return collection.aggregate(pipeline);
	}
	
	private void remove2StarsBusinesses() {
		MongoCollection<Document> collection = mdb.getCollection("business_copy2");
		Document query = new Document("stars", 2.0);
		collection.deleteMany(query);
	}
	
	private AggregateIterable<Document> getAvgBusinessStars(){
		Document businessStars = new Document("_id", new Document("business", "$business_id"));
		businessStars.put("avg", new Document("$avg", "$stars")); 
		Document group = new Document("$group", businessStars);
		
		Document match = new Document("$match", new Document("avg", new Document("$gte", 4.0)));
		
		List<Document> pipeline = Arrays.asList(group, match);
		MongoCollection<Document> collection = mdb.getCollection("business");
		
		return collection.aggregate(pipeline);
	}
	
	public static void main(String args[]) throws UnknownHostException {
		MongoHomework mongoHomework = new MongoHomework();
		
		//ZAD 6 a
		
		AggregateIterable<Document> businessCities = mongoHomework.businessCities();
		for(Document d : businessCities) {
			System.out.println(d.toJson());
		}
	
		//ZAD 6 b
		startTime = System.nanoTime();
		System.out.print("FIRST SOLUTION: Result: "+mongoHomework.moviesAfter2011());
		endTime = System.nanoTime();
		System.out.println(" Execution Time: "+(endTime-startTime)+" [ns]");
		startTime = System.nanoTime();
		System.out.print("FASTER SOLUTION: Result: "+mongoHomework.moviesAfter2011BetterSolution());
		endTime = System.nanoTime();
		System.out.println(" Execution Time: "+(endTime-startTime)+" [ns]");
		
		//ZAD 6 c
		
		AggregateIterable<Document> closedBusinesses = mongoHomework.getClosedBusinesses();
		for(Document d : closedBusinesses) {
			System.out.println(d.toJson());
		}
		
		//ZAD 6 d
		AggregateIterable<Document> noFunnyAndUsefulUsers = mongoHomework.getNoFunnyAndUsefulUsers();
		for(Document d : noFunnyAndUsefulUsers) {
			System.out.println(d.toJson());
		}
		
		//ZAD 6 e
		AggregateIterable<Document> businessTips = mongoHomework.getBusinessTips();
		for(Document d : businessTips) {
			System.out.println(d.toJson());
		}
		
		//ZAD 6 f
		AggregateIterable<Document> avgBusinessStars = mongoHomework.getAvgBusinessStars();
		for(Document d : avgBusinessStars) {
			System.out.println(d.toJson());
		}
		
		
		//ZAD 6 g
		mongoHomework.remove2StarsBusinesses();
	}
	
}
