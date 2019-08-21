package test;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import hw5.DB;
import hw5.DBCollection;
import hw5.DBCursor;
import hw5.Document;

class CursorTester {

	/*
	 * Queries: Find all (done?) Find with relational select Find with projection
	 * Conditional operators Embedded Documents and arrays
	 */

	@Test
	public void testFindAll() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find();
		assertTrue(results.count() == 3);
		assertTrue(results.hasNext());
		JsonObject d1 = results.next(); // verify contents?
		assertTrue(results.hasNext());
		JsonObject d2 = results.next();// verify contents?
		assertTrue(results.hasNext());
		JsonObject d3 = results.next();// verify contents?
		assertTrue(!results.hasNext());
	}

	// Test: Queries that request a single document from the collection
	@Test
	public void testFindWithQuery1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"key\":\"value\"}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("key").getAsJsonPrimitive().getAsString().equals("value"));
	}

	// Test: Queries based on data in an embedded document
	@Test
	public void testFindWithQuery2_embedded() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"embedded.key2\":\"value2\"}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("embedded").getAsJsonObject().get("key2").
				getAsJsonPrimitive().getAsString().equals("value2"));
	}

	//Test for <key: JsonObject> query
	@Test
	public void testFindWithQuery3_jsonobject() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"embedded\":{\"key2\":\"value2\"}}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("embedded").getAsJsonObject().get("key2").
				getAsJsonPrimitive().getAsString().equals("value2"));
	}

	// Test: Queries for <key, array>
	@Test
	public void testFindWithQuery4_jsonarray() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\r\n" + "	\"array\": [ \"one\", \"two\", \"three\" ]\r\n" + "}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("array").getAsJsonArray().get(0).
				getAsJsonPrimitive().getAsString().equals("one"));
	}

	// Test: Queries for array
	@Test
	public void testFindWithQuery5() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"array\":\"two\"}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("array").getAsJsonArray().get(0).
				getAsJsonPrimitive().getAsString().equals("one"));
	}

	// Test $gt
	@Test
	public void testFindWithQuery6() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"key\":{$gt:\"valu\"}}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("key").getAsJsonPrimitive().getAsString().equals("value"));
	}

	// Test $lt
	@Test
	public void testFindWithQuery7() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"key\":{$lt:\"value1\"}}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("key").getAsJsonPrimitive().getAsString().equals("value"));
	}

	// Test $in: value in document is JsonPrimitive
	@Test
	public void testFindWithQuery8() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"key\":{$in:[\"value1\",\"value\"]}}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("key").getAsJsonPrimitive().getAsString().equals("value"));
	}

	// Test $in: value in document is JsonArray
	@Test
	public void testFindWithQuery9() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"array\":{$in:[\"one\",\"value\"]}}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("array").getAsJsonArray().get(0).
				getAsJsonPrimitive().getAsString().equals("one"));
	}
	
	
	// ##### More complex test based on mytest.json file #####//
	
	/*
	 * Test for collection as following:
	 * [
   			{ item: "journal", qty: 25, size: { h: 14, w: 21, uom: "cm" }, status: "A" },
   			{ item: "notebook", qty: 50, size: { h: 8.5, w: 11, uom: "in" }, status: "A" },
   			{ item: "paper", qty: 100, size: { h: 8.5, w: 11, uom: "in" }, status: "D" },
   			{ item: "planner", qty: 75, size: { h: 22.85, w: 30, uom: "cm" }, status: "D" },
   			{ item: "postcard", qty: 45, size: { h: 10, w: 15.25, uom: "cm" }, status: "A" }
		]
	 * */
	@Test
	public void testFindWithQuery10() { 
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest");
		DBCursor results = test.find(Document.parse("{\"size.h\":{\"$in\":[\"18\",\"14\",\"8.5\"]}, \"item\":{\"$gte\":\"paper\"}}"));
		assertTrue(results.count() == 1);
	}
	
	@Test
	public void testFindWithQuery11() { 
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest");
		DBCursor results = test.find(Document.parse("{\"size\":{ \"h\": \"14\", \"w\": \"21\", \"uom\": \"cm\" }}"));
		//System.out.println(results.count());
		assertTrue(results.count() == 1);
		System.out.println(results.next().toString());
	}
	
	@Test
	public void testFindWithQuery12() { 
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest");
		DBCursor results = test.find(Document.parse("{ \"size.h\": { \"$ne\": \"15\" }, \"size.uom\": \"in\", \"status\": \"D\" }"));
		//System.out.println(results.count());
		assertTrue(results.count() == 1);
		System.out.println(results.next().toString());
	}
	
	// Test with array index in query key
	@Test
	public void testFindWithQuery13() { 
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest2");
		DBCursor results = test.find(Document.parse("{ \"instock.0.warehouse\": \"A\"}"));
		//System.out.println(results.count());
		assertTrue(results.count() == 3);
//		System.out.println(results.next().toString());
	}
	
	// Test with array index in query key
	@Test
	public void testFindWithQuery14() { 
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest2");
		DBCursor results = test.find(Document.parse("{ \"instock.0.qty\": {\"$lte\": 50}}"));
		System.out.println(results.count());
		assertTrue(results.count() == 4);
		while(results.hasNext()) {
			System.out.println(results.next().toString());
		}
	}
	
	@Test
	public void testFindWithEmptyQuery() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest");
		DBCursor results = test.find(Document.parse("{}"));
		//System.out.println(results.count());
		assertTrue(results.count() == 5);
	}
	
	@Test
	public void testFindWithProjection1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest");
		DBCursor results = test.find(Document.parse("{}"), Document.parse("{ \"item\": \"1\", \"status\": \"1\" }"));
		assertTrue(results.count() == 5);
		while(results.hasNext()) {
			assertTrue(results.next().has("item") == true);
		}
	}
	
	@Test
	public void testFindWithProjection2() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest");
		DBCursor results = test.find(Document.parse("{}"), Document.parse("{ \"item\": 0, \"status\": 0 }"));
		assertTrue(results.count() == 5);
		while(results.hasNext()) {
			assertTrue(results.next().has("item") == false);
		}
	}
	
//	@Test
//	public void testFindWithProjection3_Conflict() {
//		DB db = new DB("data");
//		DBCollection test = db.getCollection("mytest");
//		DBCursor results = test.find(Document.parse("{}"), Document.parse("{ \"item\": 0, \"status\": 1 }"));
//	}
	
	@Test
	public void testFind_Field() {
		
	}
	
	@Test
	public void testFindWithProjection3_embeded() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest");
		DBCursor results = test.find(Document.parse("{}"), Document.parse("{ \"item\": 1, \"size.h\": 1, \"size.w\": 1 }"));
		assertTrue(results.count() == 5);
		while(results.hasNext()) {
			System.out.println(results.next().toString());
		}
	}
	
//	@Test
//	public void temp_testSizeOfJsonObject() {
//		JsonObject test = new JsonObject();
//		System.out.print(test.size());
//	}
	
	/*
	 * @Test public void testFindWithQuery3() { DB db = new DB("data"); DBCollection
	 * test = db.getCollection("test"); DBCursor results =
	 * test.find(Document.parse("{\"key\":\"value\"}")); assertTrue(results.count()
	 * == 1); System.out.println(results.hasNext());
	 * System.out.println(results.next().toString()); }
	 */

}
