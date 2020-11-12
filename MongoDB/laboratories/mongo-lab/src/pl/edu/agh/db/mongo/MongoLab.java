package pl.edu.agh.db.mongo;

import static java.util.Arrays.asList;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoLab {

	private MongoClient mongoClient;
	private MongoDatabase mdb;
	private DB db;
	
	private static long startTime, endTime;
	
	
	public MongoLab() throws UnknownHostException{
		
		mongoClient = new MongoClient();
		db = mongoClient.getDB("BartoszKordek2");
		mdb = mongoClient.getDatabase("BartoszKordek2");
		
	}
	
	private void showCollections() {
		for(String name : db.getCollectionNames()) {
			System.out.println("collection: " + name);
		}
	}
	
	private void insertStudent() {
		DBCollection gettedCollection = db.getCollection("student"); 
		Document student = new Document("_id", new ObjectId());
		student.append("First Name", "John")
			.append("Last Name", "Smith")
			.append("Presence", new Boolean(true))
			.append("Mark", null)
			.append("Current Date", "2020-11-07")
			.append("Passed Subjects", asList("JAVA", "CONCURRENT PROGRAMMING", "ALGORITHMS"));
		
		BasicDBObject basicDBObject = new BasicDBObject(student);
		gettedCollection.insert(basicDBObject);
	}
	
	private long get5StarBusinessCounter() {
		DBCollection gettedCollection = db.getCollection("business"); 
		DBObject query = new BasicDBObject();
		query.put("stars", 5.0);
		return gettedCollection.count(query);
	}
	
	private AggregateIterable<Document> getRestaurantsByCity(){

		MongoCollection<Document> gettedCollection = mdb.getCollection("business");
		
		Document match = new Document("$match", new Document("categories", "Restaurants"));

	    Document groupCity = new Document("_id", "$city");
	    groupCity.put("quantity", new Document("$sum", 1));
	    Document group = new Document("$group", groupCity);

	    Document sort = new Document("$sort", new Document("quantity", -1));

	    List<Document> pipeline = Arrays.asList(match, group, sort);
	    return gettedCollection.aggregate(pipeline);
	}
	
	public AggregateIterable<Document> getHotelAmount(){
		
	    MongoCollection<Document> gettedCollection = mdb.getCollection("business");
	    
	    Document category = new Document("categories", "Hotels");
	    Document wifi = new Document("attributes.Wi-Fi", "free");
	    Document stars = new Document("stars", new Document("$gte", 4.5));
	    
	    Document match = new Document("$match", new Document("$and", Arrays.asList(category,wifi, stars)));

	    Document groupByFields = new Document("_id", "$state");
	    groupByFields.put("quantity", new Document("$sum", 1));
	    Document group = new Document("$group", groupByFields);

	    List<Document> pipeline = Arrays.asList(match, group);
	    
	    return gettedCollection.aggregate(pipeline);
	}
	
	
	private void getVotesCounts(){
		
		DBCollection collection = db.getCollection("user");
		
		String map = "function() {\r\n" + 
				"   if(this.votes.funny>0) emit(\"funny\", 1);\r\n" + 
				"   if(this.votes.useful>0) emit(\"useful\", 1);\r\n" + 
				"   if(this.votes.cool>0) emit(\"cool\", 1);\r\n" + 
				"};";
		
		String reduce = "function(voteCategory, votesNumber) {\r\n" + 
				"   return Array.sum(votesNumber);\r\n" + 
				"};";
		
		MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce, null, MapReduceCommand.OutputType.INLINE, null);
		MapReduceOutput out = collection.mapReduce(cmd);
		for(DBObject o : out.results()) {
			System.out.println(o.toString());
		}
	}
	
	
	private String getMostPositiveVotesUser(){
		
		MongoCollection<Document> gettedCollection = mdb.getCollection("user");
		
		Document query = new Document("average_stars", new Document("$gt", 4.5));
		
		FindIterable<Document> documents = gettedCollection.find(query);

		MongoCursor<Document> cursor = documents.iterator();
		
		int reviewMax = 0;
		String mostPositiveVotesUser = null;
		Document currentDocument = null;
		
		while(cursor.hasNext()) {
			currentDocument = cursor.next();
			if(currentDocument.getInteger("review_count")>reviewMax) {
				reviewMax = currentDocument.getInteger("review_count");
				mostPositiveVotesUser = currentDocument.getString("name");
			}
		}	
	
		return mostPositiveVotesUser;
	}
	
	private String getMostPositiveVotesUserFasterSolution(){
		
		MongoCollection<Document> gettedCollection = mdb.getCollection("user");
		
		Document filter = new Document("average_stars", new Document("$gt", 4.5));
		Document match = new Document("$match", filter);
		
		Document sort = new Document("$sort", new Document("review_count", -1));
		
		List<Document> pipeline = Arrays.asList(match, sort);
		AggregateIterable<Document> documents = gettedCollection.aggregate(pipeline);
		return documents.iterator().next().getString("name");
	}
	
	public static void main(String args[]) throws UnknownHostException {
		MongoLab mongoLab = new MongoLab();
		mongoLab.showCollections();
		startTime = System.nanoTime();
		System.out.print("FIRST SOLUTION: Result: "+mongoLab.getMostPositiveVotesUser());
		endTime = System.nanoTime();
		System.out.println(" Execution Time: "+(endTime-startTime)+" [ns]");
		startTime = System.nanoTime();
		System.out.print("FASTER SOLUTION: Result: "+mongoLab.getMostPositiveVotesUserFasterSolution());
		endTime = System.nanoTime();
		System.out.println(" Execution Time: "+(endTime-startTime)+" [ns]");
	}
}
