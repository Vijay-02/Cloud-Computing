package com.mapreduce.core;

import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 *
 *  * instances are potentially executed by another thread
 */
public abstract class Mapper implements Callable {
	private String line;
	private boolean verbose;

	/**
	 * Converts a line of a file into zero or more KeyValPair objects.
	 *
	 * @param line A line of a file.
	 * @return An ArrayList of zero or more KVPairs.
	 */
	protected abstract ArrayList<KeyValPair> map(String line);

	/**
	 * Internal method, sets the operated string.
	 *
	 * @param line A line of a file.
	 */
	void setInput(String line) {
		this.line = line;
	}

	/**
	 * Implements the Callable interface for multi-threading by the controller.
	 *
	 * @return The return value of the user-implemented this.map
	 */
	@Override
	public ArrayList<KeyValPair> call() {
		if (this.verbose) {
			System.out.println("Map { " + this.line + " } :: " + Thread.currentThread().getName());
		}
		return this.map(this.line);
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
}
