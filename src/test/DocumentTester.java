package test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import hw5.DB;
import hw5.DBCollection;
import hw5.Document;

class DocumentTester {

	/*
	 * Things to consider testing:
	 * 
	 * Parsing embedded documents
	 * Parsing arrays
	 * 
	 * Object to primitive
	 * Object to embedded document
	 * Object to array
	 */
	@Test
	public void testParse() {
		String json = "{ \"key\": \"value\" }";
		JsonObject results = Document.parse(json);
		assertTrue(results.getAsJsonPrimitive("key").getAsString().equals("value"));
	}
	
	@Test
	public void toJsonString() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		JsonObject json = test.getDocument(1);
		
		String json_str = Document.toJsonString(json);
		JsonObject results = Document.parse(json_str);
		assertTrue(results.getAsJsonObject("embedded").getAsJsonPrimitive("key2").getAsString().equals("value2"));
	}
	
	// Test with mytest.json
	@Test
	public void toJsonString1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("mytest");
		System.out.println();
		
		JsonObject json = test.getDocument(1);
		
		String json_str = Document.toJsonString(json);
		JsonObject results = Document.parse(json_str);
		assertTrue(results.getAsJsonObject("size").getAsJsonPrimitive("w").getAsString().equals("11"));
	}
}
