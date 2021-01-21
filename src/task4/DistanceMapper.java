package task4;


import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Mapper;
import com.mapreduce.util.ErrorHandler;

import java.util.ArrayList;

public class DistanceMapper extends Mapper {
	@Override
	protected ArrayList<KeyValPair> map(String line) {


		String[] values = line.split(",");

		ArrayList<KeyValPair> results = new ArrayList<>();

		if (values[0] != line) {
			String passengerID = values[0];
			String flightID = values[1];
			String fAirportCode = values[2];
			String dAirportCode = values[3];

			if (ErrorHandler.airport(dAirportCode) && ErrorHandler.airport(fAirportCode)) {
				if (ErrorHandler.flight(flightID)) {
					results.add(new KeyValPair(flightID, fAirportCode + "|" + dAirportCode));
				}
				if (ErrorHandler.passenger(passengerID)) {
					results.add(new KeyValPair(passengerID, fAirportCode + "|" + dAirportCode));
				}
			}
		}

		return results;
	}
}
