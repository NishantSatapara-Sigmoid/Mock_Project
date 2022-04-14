import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.HashMap;

public class mongocheck {

    public static void main(String[] args) {
        MongoClientURI uri = new MongoClientURI(
                "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false");
        MongoClient mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase("TwitterDB");
        MongoCollection<Document> collection = database.getCollection("Tweets");
        HashMap<String,Object> ele = new HashMap<>();
        ele.put("country",56);
        ele.put("day",56);
        ele.put("month",456);
        ele.put("tweet",78);
        Document doc = new Document(ele);
        collection.insertOne(doc);
        System.out.println("Collection myCollection selected successfully");
    }
}
