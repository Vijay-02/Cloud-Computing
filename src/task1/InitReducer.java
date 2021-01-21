package com.mapreduce.task1;

import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Reducer;

import java.util.ArrayList;

public class InitReducer extends Reducer {
	@Override
	protected KeyValPair reduce(String key, ArrayList<String> values) {
		int counter = 0;

		for (String val : values) {
			counter += Integer.parseInt(val, 10);
		}

		return new KeyValPair(key, String.valueOf(counter));
	}
}
