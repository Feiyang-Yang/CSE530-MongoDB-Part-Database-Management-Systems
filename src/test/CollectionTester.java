package test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import hw5.DB;
import hw5.DBCollection;
import hw5.DBCursor;
import hw5.Document;

class CollectionTester {

	/*
	 * Things to be tested:
	 * 
	 * Document access (done?) Document insert/update/delete
	 */
	@Test
	public void testGetDocument() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		JsonObject primitive = test.getDocument(0);
		assertTrue(primitive.getAsJsonPrimitive("key").getAsString().equals("value"));
	}

	@Test
	public void testGetCollectionNoExist() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test1");
		assertTrue(new File("testfiles/data/test1.json").exists());
	}

	@Test
	public void testInsertDocuments_1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test2 = db.getCollection("test2");
		long size_0 = test2.count();
		test2.insert(documents.toArray(documents_array));
		assertTrue(test2.count() - size_0 == 3);
	}

	/*
	 * Test insert [ { item: "journal", qty: 25, size: { h: 14, w: 21, uom: "cm" },
	 * status: "A" }, { item: "notebook", qty: 50, size: { h: 8.5, w: 11, uom: "in"
	 * }, status: "A" }, { item: "paper", qty: 100, size: { h: 8.5, w: 11, uom: "in"
	 * }, status: "D" }, { item: "planner", qty: 75, size: { h: 22.85, w: 30, uom:
	 * "cm" }, status: "D" }, { item: "postcard", qty: 45, size: { h: 10, w: 15.25,
	 * uom: "cm" }, status: "A" } ]
	 * 
	 */
	@Test
	public void testInsertDocuments_2() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest"); // mytest.json is in testfiles/data/
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test3 = db.getCollection("test3");
		long size_0 = test3.count();
		test3.insert(documents.toArray(documents_array));
		assertTrue(test3.count() - size_0 == 5);
	}

	// Every time run this test, need clear test4
	@Test
	public void testRemoveDocuments_1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest"); // mytest.json is in testfiles/data/
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test4 = db.getCollection("test4");
		test4.drop();
		test4 = db.getCollection("test4");
		long size_0 = test4.count();
		test4.insert(documents.toArray(documents_array));
		assertTrue(test4.count() - size_0 == 5);

		long size_1 = test4.count();
		test4.remove(Document.parse("{\"size.h\":{\"$in\":[\"18\",\"14\",\"8.5\"]}, \"item\":{\"$gte\":\"paper\"}}"),
				true);
		// Remove one
		assertTrue(size_1 - test4.count() == 1);
	}

	@Test
	public void testUpdateDocuments_1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest");
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test5 = db.getCollection("test5");
		test5.clear();
	
		test5.insert(documents.toArray(documents_array));
		test5.update(Document.parse("{\"size.h\":{\"$in\":[\"18\",\"14\",\"8.5\"]}, \"item\":\"paper\"}"),
				Document.parse("{\"update_key\": \"value\"}"), true);
		DBCursor results = test5
				.find(Document.parse("{\"update_key\": \"value\"}"));
		// System.out.println(results.count());
		assertTrue(results.count() == 1);
	}

	@Test
	public void testDropCollection() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest");
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test6 = db.getCollection("test6");
		test6.drop();
	}
}
