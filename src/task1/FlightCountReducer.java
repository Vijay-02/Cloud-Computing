package com.mapreduce.task1;

import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Reducer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class FlightCountReducer extends Reducer {
	@Override
	protected KeyValPair reduce(String key, ArrayList<String> values) {


		Set<String> uniques = new HashSet<>();
		uniques.addAll(values);
		return new KeyValPair(key, String.valueOf(uniques.size()));
	}
}
