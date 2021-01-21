package task2;



import com.mapreduce.core.Job;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class Main {
	public static void main(String[] args) throws Exception {
		// Create a list of flights based on the Flight id, this
		// output includes relevant flight data such as the passenger Id, IATA/FAA
		// codes, the departure and arrival time (converted to HH:MM:SS format), and the flight duration

		Job job = new Job();

		job.setMapper(FlightDetailsMapper.class);
		job.setReducer(FlightDetailsReducer.class);

		BufferedReader input = new BufferedReader(new FileReader("AComp_Passenger_data.csv"));
		BufferedWriter output = new BufferedWriter(new FileWriter("flightdetails.txt"));
		job.setInput(input);
		job.setOutput(output);

		job.run();
	}
}
