package task2;

import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Mapper;
import com.mapreduce.util.ErrorHandler;

import java.util.ArrayList;

public class FlightDetailsMapper extends Mapper {
	@Override
	protected ArrayList<KeyValPair> map(String line) {


		String[] values = line.split(",");

		ArrayList<KeyValPair> results = new ArrayList<>();

		if (!values[0].equals(line)) {
			boolean isvalid = true;

			String passengerID = values[0];
			isvalid &= ErrorHandler.passenger(passengerID);

			String flightID = values[1];
			isvalid &= ErrorHandler.flight(flightID);

			String fAirportCode = values[2];
			isvalid &= ErrorHandler.airport(fAirportCode);

			String dAirportCode = values[3];
			isvalid &= ErrorHandler.airport(dAirportCode);

			String departureTime = values[4];
			isvalid &= ErrorHandler.time(departureTime);

			String flightDuration = values[5];
			isvalid &= ErrorHandler.duration(flightDuration);

			if (isvalid) {
				results.add(new KeyValPair(flightID, String.format("%s,%s,%s,%s,%s", passengerID, fAirportCode, dAirportCode, departureTime, flightDuration)));
			}
		}

		return results;
	}
}
