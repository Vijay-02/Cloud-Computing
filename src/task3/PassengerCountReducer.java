package task3;

import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Reducer;

import java.util.ArrayList;

public class PassengerCountReducer extends Reducer {
	@Override
	protected KeyValPair reduce(String key, ArrayList<String> values) {


		int sum = 0;

		for (String count : values) {
			sum += Integer.parseInt(count, 10);
		}

		return new KeyValPair(key, String.valueOf(sum));
	}
}
