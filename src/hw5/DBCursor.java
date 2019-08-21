package hw5;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class DBCursor implements Iterator<JsonObject> {

	ArrayList<JsonObject> documents;
	int cursor; // index for documents arraylist

	// ### Create DBCursor based on query and fields ###
	// query is relational select (where); fields is relational project (select)
	public DBCursor(DBCollection collection, JsonObject query, JsonObject fields) {
		// 1. return all documents (if query and fields are null)
		if (query == null && fields == null) {
			this.documents = collection.getAllDocuments();
		}

		// 2. Handle query (if query is not null and fields are null)
		if (query != null && fields == null) {
			this.documents = handle_query(collection.getAllDocuments(), query);
		}

		// 3. Handle query first then handle projection (if query and fields are not
		// null)
		if (query != null && fields != null) {
			this.documents = handle_projection(handle_query(collection.getAllDocuments(), query), fields);
		}
	}

	// ############### Handle Query BEGIN ###############//

	// $$$Main function$$$ for Query: Get resulting documents based on query
	/*
	 * 1. Queries that request a single document from the collection
	 * db.inventory.find( { status: "D" } )
	 * 
	 * 2. Queries based on data in an embedded document or list db.inventory.find( {
	 * "size.uom": "in" } ) db.inventory.find( { tags: ["red", "blank"] } )
	 * db.inventory.find( { tags: "red" } )
	 * 
	 * 3. Queries with comparison operators (you do not have to implement the other
	 * operator types) db.inventory.find( { "size.h": { $lt: 15 } } )
	 */
	private ArrayList<JsonObject> handle_query(ArrayList<JsonObject> documents, JsonObject query) {
		// If query is empty but not null, return all documents
		if (query.size() == 0) {
			return documents;
		}

		ArrayList<JsonObject> results = new ArrayList<>();

		// Iterate all the documents and check it with query
		for (JsonObject document : documents) {
			if (document_meet_query(document, query)) {
				results.add(document);
			}
		}
		return results;
	}

	// Check if document meets the requirment of query
	private boolean document_meet_query(JsonObject document, JsonObject query) {
		Set<String> query_keys = query.keySet(); // get all the keys in query
		// Iterate the <key, value> pairs in query
		for (String query_key : query_keys) {
			String[] query_key_split = query_key.split("\\.");

			// #1. query_key doesn't have ".", compare Value directly
			if (query_key_split.length == 1) {
				if (!document.has(query_key)) { // query_key doesn't exist
					return false;
				} else { // query_key exists in this document, check value in <key, value> pair
					if (!compareValue(document.get(query_key), query.get(query_key))) {
						return false;
					}
				}
			}

			// DONE: Handle query find( { "dim_cm.1": { $gt: 25 } } ) for
			// "{ item: "journal", qty: 25, tags: ["blank", "red"], dim_cm: [ 14, 21 ] }"

			// #2. query_key have "." try to find "Value" in embeded document
			else if (query_key_split.length > 1) {
				JsonElement temp_doc = document;
				// find the "Value" by going into the embeded document step by step based on "."
				int i = 0;
				for (; i < query_key_split.length - 1; i++) {
					if (temp_doc.isJsonObject() && temp_doc.getAsJsonObject().has(query_key_split[i])) {
						if (!temp_doc.getAsJsonObject().get(query_key_split[i]).isJsonObject()
								&& !temp_doc.getAsJsonObject().get(query_key_split[i]).isJsonArray()) {
							// if next Value is not JsonObject, cannot go deep, false
							return false;
						}
						// go deep to next JsonObject
						temp_doc = temp_doc.getAsJsonObject().get(query_key_split[i]);
						continue;
					}

					// Also handle JsonArray
					if (temp_doc.isJsonArray() && this.isInteger(query_key_split[i])) {
						int index = Integer.parseInt(query_key_split[i]);
						int size = temp_doc.getAsJsonArray().size();
						if (index >= 0 && index < size) {
							if (!temp_doc.getAsJsonArray().get(index).isJsonArray()
									&& !temp_doc.getAsJsonArray().get(index).isJsonObject()) {
								return false;
							}
							temp_doc = temp_doc.getAsJsonArray().get(index);
							continue;
						} else {
							return false;
						}
					}
					return false;
				}

				// Handle last key
				if (temp_doc.isJsonObject() && temp_doc.getAsJsonObject().has(query_key_split[i])) {
					// Compare value
					if (!compareValue(temp_doc.getAsJsonObject().get(query_key_split[i]), query.get(query_key))) {
						return false;
					}
				}

				if (temp_doc.isJsonArray() && this.isInteger(query_key_split[i])) {
					int index = Integer.parseInt(query_key_split[i]);
					int size = temp_doc.getAsJsonArray().size();
					if (index >= 0 && index < size) {
						// Compare value
						if (!compareValue(temp_doc.getAsJsonArray().get(index), query.get(query_key))) {
							return false;
						}
					} else {
						return false;
					}
				}
			} else { // Length illegal
				return false;
			}
		}
		// All <key, value> in this document meet the query requirement
		return true;
	}

	// Compare value of document and query in query_key
	private boolean compareValue(JsonElement document_value, JsonElement query_value) {
		// ##### 1. value in document is JsonString #####//
		if (document_value.isJsonPrimitive()) {
			// ### 1.1 value in query is also JsonString -- compare directly
			if (query_value.isJsonPrimitive()) {
				if (document_value.getAsJsonPrimitive().equals(query_value.getAsJsonPrimitive())) {
					return true;
				}
			}
			// ### 1.2 value in query is JsonDocument -- may have comparison operators
			if (query_value.isJsonObject()) {
				// Check if value in query has comparsion operator
				Set<String> keys = query_value.getAsJsonObject().keySet();

				// $eq $gt $gte $lt $lte $ne $in $nin
				for (String operator : keys) {
					// operator value is not JsonPrimitive
					if ((operator.equals("$eq") || operator.equals("$gt") || operator.equals("$gte")
							|| operator.equals("$lt") || operator.equals("$lte") || operator.equals("$ne"))
							&& query_value.getAsJsonObject().get(operator).isJsonPrimitive()) {
						if (!comparisonOperation(operator, document_value, query_value.getAsJsonObject().get(operator)))
							return false;
					} else if (operator.equals("$in")
							|| operator.equals("$nin") && query_value.getAsJsonObject().get(operator).isJsonArray()) {
						if (!comparisonOperation(operator, document_value, query_value.getAsJsonObject().get(operator)))
							return false;
					} else { // query_value is not comparison operation
						return false;
					}
				}
				return true;
			}
			// ### 1.3 value in query is JsonArray -- found = false
			if (query_value.isJsonArray()) {
				return false;
			}
		}

		// ##### 2. value in document is JsonDocument #####//
		if (document_value.isJsonObject()) {
			// ### 2.1 value in query is JsonString -- found = false
			if (query_value.isJsonPrimitive()) {
				return false;
			}
			// ### 2.2 value in query is JsonDocument -- compare directly
			if (query_value.isJsonObject()) {
				if (document_value.getAsJsonObject().toString().equals(query_value.getAsJsonObject().toString()))
					return true;

				Set<String> keys = query_value.getAsJsonObject().keySet();

				for (String operator : keys) {
					// operator value is not JsonPrimitive
					if ((operator.equals("$in") || operator.equals("$nin"))
							&& query_value.getAsJsonObject().get(operator).isJsonArray()) {
						if (!comparisonOperation(operator, document_value, query_value.getAsJsonObject().get(operator)))
							return false;
					} else { // query_value is not comparison operation
						return false;
					}
				}
				return true;

			}
			// ### 2.3 value in query is JsonArray -- found = false
			if (query_value.isJsonArray()) {
				return false;
			}
		}

		/*
		 * db.inventory.insertMany([ { item: "journal", qty: 25, tags: ["blank", "red"],
		 * dim_cm: [ 14, 21 ] }, { item: "notebook", qty: 50, tags: ["red", "blank"],
		 * dim_cm: [ 14, 21 ] }, { item: "paper", qty: 100, tags: ["red", "blank",
		 * "plain"], dim_cm: [ 14, 21 ] }, { item: "planner", qty: 75, tags: ["blank",
		 * "red"], dim_cm: [ 22.85, 30 ] }, { item: "postcard", qty: 45, tags: ["blue"],
		 * dim_cm: [ 10, 15.25 ] } ]);
		 */

		// ##### 3. value in document is Json Array #####//
		if (document_value.isJsonArray()) {
			// ### 3.1 value in query is JsonString: db.inventory.find( { tags: "red" } )
			if (query_value.isJsonPrimitive()) {
				JsonArray document_value_array = document_value.getAsJsonArray();
				return document_value_array.contains(query_value.getAsJsonPrimitive());
			}
			// ### 3.2 value in query is JsonDocument: db.inventory.find( { dim_cm: { $gt:
			// 25 }
			// } )
			if (query_value.isJsonObject()) {
				JsonArray document_value_array = document_value.getAsJsonArray();
				if (document_value_array.contains(query_value.getAsJsonObject())) {
					return true;
				}

				// query_value may be comparison operator
				Set<String> keys = query_value.getAsJsonObject().keySet();

				for (String operator : keys) {
					if ((operator.equals("$eq") || operator.equals("$gt") || operator.equals("$gte")
							|| operator.equals("$lt") || operator.equals("$lte") || operator.equals("$ne"))
							&& query_value.getAsJsonObject().get(operator).isJsonPrimitive()) {
						for (JsonElement document_value_each : document_value_array) {
							if (!document_value_each.isJsonPrimitive()) {
								return false;
							}
							if (!comparisonOperation(operator, document_value_each,
									query_value.getAsJsonObject().get(operator)))
								return false;
						}
					} else if ((operator.equals("$in") || operator.equals("$nin"))
							&& query_value.getAsJsonObject().get(operator).isJsonArray()) {
						if (!comparisonOperation(operator, document_value, query_value.getAsJsonObject().get(operator)))
							return false;
					} else { // query_value is not comparison operation
						return false;
					}
				}
				return true;
			}
			// ### 3.3 value in query is JsonArray: db.inventory.find( { tags: ["red",
			// "blank"]
			// } ) toString
			if (query_value.isJsonArray()) {
				return document_value.getAsJsonArray().toString().equals(query_value.getAsJsonArray().toString());
			}
		}
		return false;
	}

	// Do comparison operation $eq $gt $gte $lt $lte $ne $in $nin
	// Comarison operator:
	/*
	 * $in $nin Matches any of the values specified in an array.
	 *
	 * db.inventory.find( { status: { $in: [ "A", "D" ] } } )
	 * 
	 * Useful functions for this part: isJsonArray, isJsonObject, isJsonPrimitive
	 */
	private boolean comparisonOperation(String operator, JsonElement doc_value, JsonElement query_value) {
		if (operator.equals("$eq")) {
			return doc_value.getAsJsonPrimitive().getAsString()
					.compareTo(query_value.getAsJsonPrimitive().getAsString()) == 0;
		}

		if (operator.equals("$gt")) {
			// is number
			if (doc_value.getAsJsonPrimitive().isNumber() && query_value.getAsJsonPrimitive().isNumber()) {
				return doc_value.getAsJsonPrimitive().getAsDouble() > query_value.getAsJsonPrimitive().getAsDouble();
			}

			// is string
			return doc_value.getAsJsonPrimitive().getAsString()
					.compareTo(query_value.getAsJsonPrimitive().getAsString()) > 0;
		}

		if (operator.equals("$gte")) {
			if (doc_value.getAsJsonPrimitive().isNumber() && query_value.getAsJsonPrimitive().isNumber()) {
				return doc_value.getAsJsonPrimitive().getAsDouble() >= query_value.getAsJsonPrimitive().getAsDouble();
			}

			return doc_value.getAsJsonPrimitive().getAsString()
					.compareTo(query_value.getAsJsonPrimitive().getAsString()) >= 0;
		}

		if (operator.equals("$lt")) {
			if (doc_value.getAsJsonPrimitive().isNumber() && query_value.getAsJsonPrimitive().isNumber()) {
				return doc_value.getAsJsonPrimitive().getAsDouble() < query_value.getAsJsonPrimitive().getAsDouble();
			}

			return doc_value.getAsJsonPrimitive().getAsString()
					.compareTo(query_value.getAsJsonPrimitive().getAsString()) < 0;
		}

		if (operator.equals("$lte")) {
			if (doc_value.getAsJsonPrimitive().isNumber() && query_value.getAsJsonPrimitive().isNumber()) {
				return doc_value.getAsJsonPrimitive().getAsDouble() <= query_value.getAsJsonPrimitive().getAsDouble();
			}

			return doc_value.getAsJsonPrimitive().getAsString()
					.compareTo(query_value.getAsJsonPrimitive().getAsString()) <= 0;
		}

		if (operator.equals("$ne")) {
			if (doc_value.getAsJsonPrimitive().isNumber() && query_value.getAsJsonPrimitive().isNumber()) {
				return doc_value.getAsJsonPrimitive().getAsDouble() != query_value.getAsJsonPrimitive().getAsDouble();
			}

			return doc_value.getAsJsonPrimitive().getAsString()
					.compareTo(query_value.getAsJsonPrimitive().getAsString()) != 0;
		}

		if (operator.equals("$in")) {
			if (doc_value.isJsonPrimitive()) {
				return query_value.getAsJsonArray().contains(doc_value.getAsJsonPrimitive());
			}

			if (doc_value.isJsonObject()) {
				return query_value.getAsJsonArray().contains(doc_value.getAsJsonObject());
			}

			if (doc_value.isJsonArray()) {
				JsonArray doc_value_array = doc_value.getAsJsonArray();
				for (JsonElement doc_value_each : doc_value_array) {
					if (query_value.getAsJsonArray().contains(doc_value_each)) {
						return true;
					}
				}
				return false;
			}
		}

		if (operator.equals("$nin")) {
			if (doc_value.isJsonPrimitive()) {
				return !query_value.getAsJsonArray().contains(doc_value.getAsJsonPrimitive());
			}

			if (doc_value.isJsonObject()) {
				return !query_value.getAsJsonArray().contains(doc_value.getAsJsonObject());
			}

			if (doc_value.isJsonArray()) {
				JsonArray doc_value_array = doc_value.getAsJsonArray();
				for (JsonElement doc_value_each : doc_value_array) {
					if (query_value.getAsJsonArray().contains(doc_value_each)) {
						return false;
					}
				}
				return true;
			}
		}

		return false;
	}

	// ############### Handle Query END ###############//

	// ############### Handle Projection BEGIN ###############//

	// Get resulting documents ArrayList based on fields
	private ArrayList<JsonObject> handle_projection(ArrayList<JsonObject> documents, JsonObject fields) {
		// TODO
		ArrayList<JsonObject> results = new ArrayList<>();

		// Iterate all the documents and get projection result
		for (JsonObject document : documents) {
			JsonObject result;
			try {
				result = get_document_fields(document, fields);
				if (result != null) { // Not null means there are fields
					results.add(result);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return results;
	}

	// Get <key, value> pair from a document based on fields
	private JsonObject get_document_fields(JsonObject document, JsonObject fields) throws Exception {
		JsonObject result = new JsonObject(); // This is the document after fields filter
		Set<String> field_keys = fields.keySet();
		Boolean inclusion = true;

		// #1 Check fields are inclusion or exclusion
		for (String fileds_key : field_keys) {
			JsonPrimitive fields_value = fields.get(fileds_key).getAsJsonPrimitive();
			if (fields_value.isString() && fields_value.getAsString().equals("0")) {
				inclusion = false; // Exclusion
				break;
			}

			if (fields_value.isNumber() && fields_value.getAsNumber().doubleValue() == 0) {
				inclusion = false; // Exclusion
				break;
			}
			break; // Inclusion
		}

		// #2 Iterate all the keys in fields and check if it is embedded projection
		if (inclusion) { // Inclusion: field value should be all 1 except _id
			for (String fields_key : field_keys) {
				JsonPrimitive fields_value = fields.get(fields_key).getAsJsonPrimitive();
				if (fields_value.isString() && fields_value.getAsString().equals("0")) {
					// Inlcusion and Exclusion ###Conflict###
					if (fields_key.equals("_id")) {
						continue;
					}
					throw new Exception("Inclusion and Exclusion Conflict");
				}

				if (fields_value.isNumber() && fields_value.getAsNumber().doubleValue() == 0) {
					if (fields_key.equals("_id")) {
						continue;
					}
					throw new Exception("Inclusion and Exclusion Conflict");
				}

				// Split the fields_keys and check if it is embedde fields
				String[] fields_key_split = fields_key.split("\\.");
				if (fields_key_split.length == 1) { // add directly.
					if (document.has(fields_key_split[0])) {
						result.add(fields_key_split[0], document.get(fields_key_split[0]));
					}
				} else {
					JsonObject temp_obj = find_field_with_dot(document, fields_key_split);
					if (temp_obj == null) {
						continue;
					}
					JsonObject temp = result;
					JsonElement value = temp_obj.get(fields_key_split[0]);
					String key = fields_key_split[0];
					int j = 0;
					while (temp.has(key)) {
						temp = (JsonObject) temp.get(key);
						j++;
						key = fields_key_split[j];
						value = ((JsonObject) value).get(key);
					}

					temp.add(key, value);
				}
			}
		} else { // Exclusion: field value should be all 0 except "_id"
			// First, get all the <key, value> pair, then delete based on fields
			result = document;
			for (String fields_key : field_keys) {
				JsonPrimitive fields_value = fields.get(fields_key).getAsJsonPrimitive();
				if (fields_value.isString() && fields_value.getAsString().equals("1")) {
					// Inlcusion and Exclusion ###Conflict###
					if (fields_key.equals("_id")) {
						continue;
					}
					throw new Exception("Inclusion and Exclusion Conflict");
				}

				if (fields_value.isNumber() && fields_value.getAsNumber().doubleValue() == 1) {
					if (fields_key.equals("_id")) {
						continue;
					}
					throw new Exception("Inclusion and Exclusion Conflict");
				}

				// Split the fields_keys and check if it is embedde fields
				String[] fields_key_split = fields_key.split("\\.");

				if (fields_key_split.length == 1) {
					if (document.has(fields_key_split[0])) {
						result.remove(fields_key_split[0]);
					}
				} else {
					JsonObject temp_obj = find_field_with_dot(document, fields_key_split);
					// TODO Remove temp_obj in document
				}
			}
		}

		if (result.size() == 0) {
			return null;
		}
		return result;
	}

	// Find a field in a document and return a JsonObject
	private JsonObject find_field_with_dot(JsonObject document, String[] fields_key_split) {
		// Split the fields_keys and check if it is embedde fields
		JsonObject result = new JsonObject();
		JsonElement value = null;

		// Iterate the split keys and find the right value and create a embeded <key, value> pair
		JsonElement temp_doc = document;
		// Types used to record json type in each level, convenient for reconstruct
		ArrayList<Integer> types = new ArrayList<>(); // 1: JsonObject; 2: JsonArray
		int i = 0;
		for (; i < fields_key_split.length - 1; i++) {
			if (temp_doc.isJsonObject() && temp_doc.getAsJsonObject().has(fields_key_split[i])) {
				if (!temp_doc.getAsJsonObject().get(fields_key_split[i]).isJsonObject()
						&& !temp_doc.getAsJsonObject().get(fields_key_split[i]).isJsonArray()) {
					// if next Value is not JsonObject, cannot go deep, false
					return null;
				}
				types.add(1); // add 1: JsonObject type to list
				temp_doc = temp_doc.getAsJsonObject().get(fields_key_split[i]);
				continue;
			}

			if (temp_doc.isJsonArray() && this.isInteger(fields_key_split[i])) {
				int index = Integer.parseInt(fields_key_split[i]);
				int size = temp_doc.getAsJsonArray().size();
				if (index >= 0 && index < size) {
					if (!temp_doc.getAsJsonArray().get(index).isJsonArray()
							&& !temp_doc.getAsJsonArray().get(index).isJsonObject()) {
						return null;
					}
					types.add(2); // add 2: JsonArray type to list
					temp_doc = temp_doc.getAsJsonArray().get(index);
					continue;
				} else {
					return null;
				}
			}
			return null;
		}

		// Handle last key
		if (temp_doc.isJsonObject() && temp_doc.getAsJsonObject().has(fields_key_split[i])) {
			// Get the value
			types.add(1);
			value = temp_doc.getAsJsonObject().get(fields_key_split[i]);
		}

		if (temp_doc.isJsonArray() && this.isInteger(fields_key_split[i])) {
			int index = Integer.parseInt(fields_key_split[i]);
			int size = temp_doc.getAsJsonArray().size();
			if (index >= 0 && index < size) {
				types.add(2);
				value = temp_doc.getAsJsonArray().get(index);
			} else {
				return null;
			}
		}

		if (value == null) {
			return null;
		}

		// Re-construct the document
		JsonElement newValue = value;
		for (int j = fields_key_split.length - 1; j >= 0; j--) {
			String key = fields_key_split[j];
			if (types.get(j) == 1) { // Construct a JsonObject
				JsonObject temp_val = new JsonObject();
				temp_val.add(key, newValue);
				newValue = temp_val;
			} else { // Construct a JsonArray
				JsonArray temp_val = new JsonArray();
				temp_val.add(newValue);
				newValue = temp_val;
			}
		}
		result = (JsonObject) newValue;
		return result;
	} // END find_field_with_dot

	// ############### Handle Projection END ###############//

	/**
	 * Returns true if there are more documents to be seen
	 */
	public boolean hasNext() {
		return cursor < this.documents.size();
	}

	/**
	 * Returns the next document
	 */
	public JsonObject next() {
		int index = this.cursor;
		this.cursor++;
		return this.documents.get(index);
	}

	/**
	 * Returns the total number of documents
	 */
	public long count() {
		return this.documents.size();
	}

	// Check if a string is Integer
	private boolean isInteger(String str) {
		try {
			int d = Integer.parseInt(str);
		} catch (NumberFormatException | NullPointerException nfe) {
			return false;
		}
		return true;
	}
}
