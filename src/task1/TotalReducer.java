package com.mapreduce.task1;

import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Reducer;

import java.util.ArrayList;

public class TotalReducer extends Reducer {
	@Override
	protected KeyValPair reduce(String key, ArrayList<String> values) {
		// Input	: key, <"0", "1", "2">
		// Output	: <key, "3">

		int count = 0;

		for (String val : values) {
			count += Integer.parseInt(val, 10);
		}

		return new KeyValPair(key, String.valueOf(count));
	}
}
