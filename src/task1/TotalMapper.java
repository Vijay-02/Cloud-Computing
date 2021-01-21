package com.mapreduce.task1;

import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Mapper;
import com.mapreduce.util.ErrorHandler;


import java.util.ArrayList;

public class TotalMapper extends Mapper {

	@Override
	protected ArrayList<KeyValPair> map(String line) {
		// Headers	: Airport, Flights
		// Line		: DEN,1
		// Output	: <"DEN", "1">

		String[] values = line.split(",");

		ArrayList<KeyValPair> FinalResults = new ArrayList<>();

		if (values[0] != line) {
			String code = values[0];
			String subtotal = values[1];

			if (ErrorHandler.airport(code)) {
				FinalResults.add(new KeyValPair(code, subtotal));
			}
		}

		return FinalResults;
	}
}
