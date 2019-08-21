package test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

import org.junit.jupiter.api.Test;

import hw5.DB;

class DBTester {

	/*
	 * Things to consider testing:
	 * 
	 * Properly returns collections
	 * Properly creates new collection
	 * Properly creates new DB (done)
	 * Properly drops db
	 */
	@Test
	void testCreateNewDB() {
		DB hw5 = new DB("hw5");
		assertTrue(new File("testfiles/hw5").exists());
	}
	
	// Ours
	@Test
	void testDropDB() {
		DB hw5 = new DB("hw5");
		hw5.dropDatabase();
		assertTrue(!new File("testfiles/hw5").exists());
	}

}
