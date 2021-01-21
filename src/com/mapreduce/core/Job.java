package com.mapreduce.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Job {


	private final HashMap<String, String> reduceResults = new HashMap<>();
	private final HashMap<String, ArrayList<String>> mapResults = new HashMap<>();
	private Class<? extends Reducer> reducerClass;
	private Class<? extends Mapper> mapperClass;
	private BufferedReader input;
	private BufferedWriter output;
	private boolean verbose;

	public Job() {
		this(true);
	}

	public Job(boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * Specifies the input stream for the upcoming job
	 *
	 * @param input A stream that reads the selected input for each line.
	 */
	public void setInput(BufferedReader input) {
		this.input = input;
	}

	/**
	 * Configures a class of type mappable to utilise multithreading
	 *
	 * @param mapperClass An extension of class mappable.
	 */
	public void setMapper(Class<? extends Mapper> mapperClass) {
		this.mapperClass = mapperClass;
	}

	/**
	 * Configures a class of type reducible to utilise multithreadin
	 *
	 * @param reducerClass An extension of class reucible.
	 */
	public void setReducer(Class<? extends Reducer> reducerClass) {
		this.reducerClass = reducerClass;
	}

	/**
	 * Use to initate the MapReducer along with the additional user input.
	 */
	public void run() {
		try {
			this.map();
			this.reduce();
			this.finish();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Runs the map method via a ExecutorService pool (multi-threaded).
	 * All threads available on the machine are used by default
	 *
	 * @throws NoSuchMethodException
	 * @throws IOException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	private void map() throws NoSuchMethodException, IOException, IllegalAccessException, InvocationTargetException, InstantiationException, ExecutionException, InterruptedException {
		String line;
		BufferedReader input = this.input;
		Constructor constructor = this.mapperClass.getConstructor();

		int cpuThreads = Runtime.getRuntime().availableProcessors();
		ExecutorService pool = Executors.newFixedThreadPool(cpuThreads);
		Set<Future<ArrayList<KeyValPair>>> set = new HashSet<>();
		System.out.println("start the map operation");

		while ((line = input.readLine()) != null) {
			Mapper mapper = (Mapper) constructor.newInstance();
			mapper.setInput(line);
			mapper.setVerbose(this.verbose);
			Future f = pool.submit(mapper);
			set.add(f);
		}

		for (Future<ArrayList<KeyValPair>> future : set) {
			this.appendMapResults(future.get());
		}

		pool.shutdown();
	}

	/**
	 * Runs the reduce method via a ExecutorService pool (multi-threaded).
	 * All threads available on the machine are used by default
	 *
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	private void reduce() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, ExecutionException, InterruptedException {
		Constructor constructor = this.reducerClass.getConstructor();
		System.out.println("start the reduce operation");
		int cpuThreads = Runtime.getRuntime().availableProcessors();
		ExecutorService pool = Executors.newFixedThreadPool(cpuThreads);
		Set<Future<KeyValPair>> set = new HashSet<>();

		for (HashMap.Entry<String, ArrayList<String>> entry : this.mapResults.entrySet()) {
			Reducer reducer = (Reducer) constructor.newInstance();
			reducer.setData(entry.getKey(), entry.getValue());
			reducer.setVerbose(this.verbose);

			Future f = pool.submit(reducer);
			set.add(f);
		}

		for (Future<KeyValPair> future : set) {
			this.appendReduceResults(future.get());
		}

		pool.shutdown();
	}

	/**
	 * Writes results that'd been stored to the output buffer.
	 */
	private void finish() throws IOException {
		String contents = "";

		for (HashMap.Entry<String, String> result : this.reduceResults.entrySet()) {
			KeyValPair kvp = new KeyValPair(result.getKey(), result.getValue());
			contents += String.format("%s\n", kvp);
		}
		System.out.println("finalize and Write task result to a file");
		BufferedWriter output = this.output;
		output.write(contents);
		output.close();
	}

	/**
	 * Stores an output buffer to which results are written.
	 *
	 * @param output The output buffer.
	 */
	public void setOutput(BufferedWriter output) {
		this.output = output;
	}

	/**
	 * Writes an ArrayList of KeyValPair map results into memory to be reduced later on.
	 * Results are split by each key into an array.
	 *
	 * @param results An ArrayList of zero or more KeyValPairs.
	 */
	private void appendMapResults(ArrayList<KeyValPair> results) {
		for (KeyValPair result : results) {

			ArrayList<String> values = this.mapResults.get(result.getKey());
			if (values == null) {
				this.mapResults.put(result.getKey(), new ArrayList<>());
			}

			this.mapResults.get(result.getKey()).add(result.getValue());
		}
	}

	/**
	 * Stores reduce results in memory for later writing to disk.
	 *
	 * @param result Each KeyValPair represents a reduction result.
	 */
	private void appendReduceResults(KeyValPair result) {
		this.reduceResults.put(result.getKey(), result.getValue());
	}
}
