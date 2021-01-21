package task3;



import com.mapreduce.core.Job;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Calculate the number of passengers on each flight
 */
class Main {

	public static void main(String[] args) throws Exception {
		Job job = new Job();

		job.setMapper(PassengerCountMapper.class);
		job.setReducer(PassengerCountReducer.class);

		BufferedReader input = new BufferedReader(new FileReader("AComp_Passenger_data.csv"));
		BufferedWriter output = new BufferedWriter(new FileWriter("passenger-counts.txt"));
		job.setInput(input);
		job.setOutput(output);

		job.run();
	}
}
