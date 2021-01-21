package task4;



import com.mapreduce.core.Job;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class Main {
	public static void main(String[] args) throws Exception {
		// Calculate the line-of-sight miles for each flight and the total travelled by each passenger

		Job job = new Job();

		job.setMapper(DistanceMapper.class);
		job.setReducer(DistanceReducer.class);

		BufferedReader input = new BufferedReader(new FileReader("AComp_Passenger_data.csv"));
		BufferedWriter output = new BufferedWriter(new FileWriter("distances_total.txt"));

		job.setInput(input);
		job.setOutput(output);

		job.run();
	}
}
