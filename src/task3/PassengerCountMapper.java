package task3;


import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Mapper;
import com.mapreduce.util.ErrorHandler;

import java.util.ArrayList;

public class PassengerCountMapper extends Mapper {

	protected ArrayList<KeyValPair> map(String line) {


		String[] values = line.split(",");

		ArrayList<KeyValPair> finalResults = new ArrayList<>();

		if (values[0] != line) {
			String flightID = values[1];

			if (ErrorHandler.flight(flightID)) {
				finalResults.add(new KeyValPair(flightID, String.valueOf(1)));
			}
		}

		return finalResults;
	}
}
