package task2;

import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Reducer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class FlightDetailsReducer extends Reducer {
	@Override
	protected KeyValPair reduce(String key, ArrayList<String> values) {
		String[] attributes = values.get(0).split(",");


		// Create a list of flights based on the Flight id, this
		// output includes relevant flight data such as the passenger Id, IATA/FAA
		// codes, the departure and arrival time (converted to HH:MM:SS format), and the flight duration


		String fAirport = attributes[1];
		String dAirport = attributes[2];
		String departureRAW = attributes[3];
		String flightdurationRAW = attributes[4];

		int timestamp = Integer.parseInt(departureRAW, 10);
		int duration = Integer.parseInt(flightdurationRAW, 10);

		Date departed = new Date((long) timestamp * 1000);
		SimpleDateFormat daformat = new SimpleDateFormat("HH:mm:ss, dd-MM-yyyy");
		String departureDate = daformat.format(departed);
		Date arrivalDate = new Date(departed.getTime() + (duration * 60 * 1000));
		String arrivedAt = daformat.format(arrivalDate);
		int hours = duration / 60;
		int minutes = duration % 60;

		String durationOfFlight = String.format("Flight duration will be %d hours and %d minutes", hours, minutes);

		ArrayList<String> passengers = new ArrayList<>();

		for (String value : values) {
			passengers.add(value.split(",")[0]);
		}

		String passenger = String.join("\n\t", passengers);
		String val = "";

		try {
			val = String.format(
					" fAirport %s dAirport %s\n" +
					"Departed: %s (GMT)\n" +
					"Arrived: %s (GMT)\n" +
					"Duration: %s\n" +
					"Passengers: \n\t%s\n\n",
					fAirport,
					dAirport,
					departureDate,
					arrivedAt,
					durationOfFlight,
					passenger);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return new KeyValPair(key, val);
	}
}
