package hw5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;

public class DBCollection {
	
	DB database; // DB reference
	String name;
	String dir; // file path for this collection in disk
	ArrayList<JsonObject> documents; // Documents list, document is also a JsonObject

	/**
	 * Constructs a collection for the given database with the given name. If that
	 * collection doesn't exist it will be created.
	 */
	public DBCollection(DB database, String name){
		this.database = database;
		this.name = name;
		dir = database.getDir() + "/" + name + ".json"; // dir has "*.json"
		documents = new ArrayList<>();
		
		// If "*.json" doesn't exist, create one
		if(!new File(dir).exists()) {
			try {
				new File(dir).createNewFile();
				return;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// If "*.json" exists, read documents from the disk
		try {
			readDocuments();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// Read documents from this collection file "*.json"
	private void readDocuments() throws Exception {
		// Get documents(JsonObject) in this Collection
		// Sample From Geeksforgeeks
		// https://www.geeksforgeeks.org/different-ways-reading-text-file-java/
		File collection_file = new File(dir);
		BufferedReader reader;
		reader = new BufferedReader(new FileReader(collection_file));

		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = reader.readLine()) != null) {
			if(line.equals("\t")) {
				documents.add(Document.parse(sb.toString()));
				sb.setLength(0);
				continue;
			}
			sb.append(line+"\n");
		}
		documents.add(Document.parse(sb.toString()));
		
		/*
		 * // Transfer the StringBuilder to String and split them into different documents string
		String content = sb.toString();
		String[] doc_strings = content.split("}\\n\\t");
		
		// For each document string, parse into JsonObject and put into Documents list
		int i = 0;
		for(; i < doc_strings.length - 1; i++) {
			Documents.add(Document.parse(doc_strings[i] + "}"));
		}
		Documents.add(Document.parse(doc_strings[i]));
		 * */
		
		reader.close();
	}
	
	/**
	 * Returns a cursor for all of the documents in this collection.
	 */
	public DBCursor find() {
		//* Queries that include all documents from the collection -- DONE
		return new DBCursor(this, null, null);
	}

	/**
	 * Finds documents that match the given query parameters.
	 * 
	 * @param query relational select
	 * @return
	 */
	public DBCursor find(JsonObject query) {
		return new DBCursor(this, query, null);
	}

	/**
	 * Finds documents that match the given query parameters.
	 * 
	 * @param query      relational select
	 * @param projection relational project
	 * @return
	 */
	public DBCursor find(JsonObject query, JsonObject projection) {
		return new DBCursor(this, query, projection);
	}

	/**
	 * Inserts documents into the collection Must create and set a proper id before
	 * insertion When this method is completed, the documents should be permanently
	 * stored on disk.
	 * 
	 * @param documents
	 */
	public void insert(JsonObject... documents) {
		//TODO: Each document inserted into the collection should have a id field
		
		// 1. Iterate the documents array and add id property to each document before add into list
		for(JsonObject document: documents) {
			String id = this.name + ":" + this.documents.size();
			document.addProperty("_id", id);
			this.documents.add(document);
		}
		
		// 2. Write into disk
		writeIntoDisk();
	}

	/**
	 * Locates one or more documents and replaces them with the update document.
	 * 
	 * @param query  relational select for documents to be updated
	 * @param update the document to be used for the update
	 * @param multi  true if all matching documents should be updated false if only
	 *               the first matching document should be updated
	 */
	public void update(JsonObject query, JsonObject update, boolean multi) {
		DBCursor cursor = this.find(query);
		
		if(multi) {
			while(cursor.hasNext()) {
				JsonObject document_rm = cursor.next();
				int index = this.documents.indexOf(document_rm);
				this.documents.remove(index);
				this.documents.add(index, update);
			}
		}else {
			if(cursor.hasNext()) {
				JsonObject document_rm = cursor.next();
				int index = this.documents.indexOf(document_rm);
				this.documents.remove(index);
				this.documents.add(index, update);
			}
		}
		
		// Write into disk, overwrite
		writeIntoDisk();
	}

	/**
	 * Removes one or more documents that match the given query parameters
	 * 
	 * @param query relational select for documents to be removed
	 * @param multi true if all matching documents should be updated false if only
	 *              the first matching document should be updated
	 */
	public void remove(JsonObject query, boolean multi) {
		//Use find and DBCursor 
		DBCursor cursor = this.find(query);
		
		if(multi) {
			while(cursor.hasNext()) {
				JsonObject document_rm = cursor.next();
				this.documents.remove(document_rm);
			}
		}else {
			if(cursor.hasNext()) {
				JsonObject document_rm = cursor.next();
				this.documents.remove(document_rm);
			}
		}
		
		// Write into disk, overwrite
		writeIntoDisk();
	}

	/**
	 * Returns the number of documents in this collection
	 */
	public long count() {
		return this.documents.size();
	}

	public String getName() {
		return this.name;
	}

	/**
	 * Returns the ith document in the collection. Documents are separated by a line
	 * that contains only a single tab (\t) Use the parse function from the document
	 * class to create the document object
	 */
	public JsonObject getDocument(int i) {
		return this.documents.get(i);
	}

	/**
	 * Drops this collection, removing all of the documents it contains from the DB
	 */
	public void drop() {
		File collection_file = new File(this.dir);
		collection_file.delete();
		this.documents.clear();
	}
	
	// Clear all the documents
	public void clear() {
		this.documents.clear();
		writeIntoDisk();
	}
	
	private void writeIntoDisk() {
		File collection_file = new File(this.dir);
		FileOutputStream fileOutputStream;
		try {
			// Iterate all the documents and combine them into one string using "\t" to separate documents
			StringBuilder write_to_disk = new StringBuilder();
			
			if(this.documents.size() != 0) {
				int i = 0;
				for(; i < this.documents.size() - 1; i++) {
					// Here may have error in "\t" or "\\t"
					write_to_disk.append(Document.toJsonString(this.documents.get(i)) + "\n\t\n");
				}
				write_to_disk.append(Document.toJsonString(this.documents.get(i)));
			}
			
			// use FileOutputStream write string into disk
			fileOutputStream = new FileOutputStream(collection_file, false); // false to overwrite
			fileOutputStream.write(write_to_disk.toString().getBytes());
			fileOutputStream.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// Get all documents
	public ArrayList<JsonObject> getAllDocuments(){
		return this.documents;
	}
}
