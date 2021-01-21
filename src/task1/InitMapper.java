package com.mapreduce.task1;

import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Mapper;
import com.mapreduce.util.ErrorHandler;


import java.util.ArrayList;

/**
 * handel errors in the input file
 */
public class InitMapper extends Mapper {
	@Override
	protected ArrayList<KeyValPair> map(String line) {


		String[] values = line.split(",");

		ArrayList<KeyValPair> results = new ArrayList<>();

		if (values[0] != line) {
			String fromAirportCode = values[1];

			if (ErrorHandler.airport(fromAirportCode)) {
				results.add(new KeyValPair(fromAirportCode, String.valueOf(0)));
			}
		}

		return results;
	}
}
