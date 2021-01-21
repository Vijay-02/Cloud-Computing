package com.mapreduce.core;

public class KeyValPair {
	private final String key;
	private final String value;

	/**
	 * Creates a new KeyValPair.
	 *
	 * @param key   Represents the key.
	 * @param value Represents the value.
	 */
	public KeyValPair(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return this.key;
	}

	public String getValue() {
		return this.value;
	}

	/**
	 * Converts the KeyValPair into a CSV-style string.
	 *
	 * @return String representation.
	 */
	public String toString() {
		return this.key + "," + this.value;
	}
}
