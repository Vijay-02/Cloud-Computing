package com.mapreduce.task1;

import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Mapper;
import com.mapreduce.util.ErrorHandler;


import java.util.ArrayList;

public class FlightCountMapper extends Mapper {
	protected ArrayList<KeyValPair> map(String line) {


		String[] values = line.split(",");

		ArrayList<KeyValPair> finalResults = new ArrayList<>();

		if (values[0] != line) {
			String flight = values[1];
			String fcode = values[2];

			if (ErrorHandler.airport(fcode) && ErrorHandler.flight(flight)) {
				finalResults.add(new KeyValPair(fcode, flight));
			}

		}

		return finalResults;

	}
}
