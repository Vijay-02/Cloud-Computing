package com.mapreduce.task1;

import com.mapreduce.core.Job;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

class Main {

	public static void main(String[] args) throws Exception {
		Main.job1();
		Main.job2();
		Main.job3();
	}

	// Lists all existing airports as having a total flight count of 0.
	public static void job1() throws Exception {
		Job job = new Job();

		job.setMapper(InitMapper.class);
		job.setReducer(InitReducer.class);

		BufferedReader input = new BufferedReader(new FileReader("Top30_airports_LatLong.csv"));
		BufferedWriter output = new BufferedWriter(new FileWriter("task1_flightcount.txt"));

		job.setInput(input);
		job.setOutput(output);

		job.run();
	}

	// Appends the zero-totals with the real values from the data.
	public static void job2() throws Exception {
		Job job = new Job();

		job.setMapper(FlightCountMapper.class);
		job.setReducer(FlightCountReducer.class);

		BufferedReader input = new BufferedReader(new FileReader("AComp_Passenger_data.csv"));
		BufferedWriter output = new BufferedWriter(new FileWriter("task1_flightcount.txt", true));
		job.setInput(input);
		job.setOutput(output);

		job.run();
	}

	// Combines the totals.
	public static void job3() throws Exception {
		Job job = new Job();

		job.setMapper(TotalMapper.class);
		job.setReducer(TotalReducer.class);

		BufferedReader input = new BufferedReader(new FileReader("task1_flightcount.txt"));
		BufferedWriter output = new BufferedWriter(new FileWriter("task1_flightcount_final.txt"));
		job.setInput(input);
		job.setOutput(output);

		job.run();
	}

}
