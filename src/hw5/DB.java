package hw5;

import java.io.File;
import java.util.HashMap;

public class DB {

	private HashMap<String, DBCollection> collections;
	private String dir;
	/**
	 * Creates a database object with the given name.
	 * The name of the database will be used to locate
	 * where the collections for that database are stored.
	 * For example if my database is called "library",
	 * I would expect all collections for that database to
	 * be in a directory called "library".
	 * 
	 * If the given database does not exist, it should be
	 * created.
	 */
	public DB(String name) {
		this.dir = "testfiles/" + name;
		File DB_root = new File("testfiles/" + name);
		this.collections = new HashMap<>();
		
		// If directory doesn't exist, create one
		if(!DB_root.exists()) {
			new File("testfiles/" + name).mkdirs();
			return;
		}
		
		// Read *.json file in dir, transfer to DBCollection and save into collections
		String[] DB_collections = DB_root.list();  //DB_files is the name list of collections with "*.json"
		for(String collection_name : DB_collections) {
			String name_without_ext = collection_name.split("\\.")[0];
			
			// Get DBCollection from disk, create DBCollection Object and put into HashMap
			DBCollection collection = new DBCollection(this, name_without_ext);
			this.collections.put(name_without_ext, collection);
		}
	}
	
	/**
	 * Retrieves the collection with the given name
	 * from this database. The collection should be in
	 * a single file in the directory for this database.
	 * 
	 * Note that it is not necessary to read any data from
	 * disk at this time. Those methods are in DBCollection.
	 */
	public DBCollection getCollection(String name) { //name without ".json"
		// Get the DBCollection from HashMap, if there is no this collection
		if(!this.collections.containsKey(name)) {
			this.collections.put(name, new DBCollection(this, name));
		}
		
		return this.collections.get(name);
	}
	
	/**
	 * Drops this database and all collections that it contains
	 */
	public void dropDatabase() {
		// Delete everything in this dir 
		// Sample from Stackoverflow 
		// https://stackoverflow.com/questions/20281835/how-to-delete-a-folder-with-files-using-java
		File DB_root = new File(this.dir);
		
		String[] DB_collections = DB_root.list();
		for(String collection_name : DB_collections){
		    File file = new File(DB_root.getPath(), collection_name);
		    file.delete();
		}
		DB_root.delete();
		this.collections.clear();
	}
	
	// Get the directory of this database
	public String getDir() {
		return this.dir;
	}
}
