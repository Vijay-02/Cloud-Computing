package com.mapreduce.util;

public class ErrorHandler {


	private static void failed(String input, String context) {
		System.out.println(String.format("'%s' unfortunately failed the %s", input, context));
	}

	public static boolean flight(String input) {
		if (input.matches("[A-Z]{3}[0-9]{4}[A-Z]")) {
			return true;
		} else {
			ErrorHandler.failed(input, "Flight Validity test");
			return false;
		}
	}

	public static boolean airport(String input) {
		if (input.matches("[A-Z]{3}")) {
			return true;
		} else {
			ErrorHandler.failed(input, "Airport validity test");
			return false;
		}
	}

	public static boolean passenger(String input) {
		if (input.matches("[A-Z]{3}[0-9]{4}[A-Z]{2}[0-9]")) {
			return true;
		} else {
			ErrorHandler.failed(input, "Passenger code validity test");
			return false;
		}
	}

	public static boolean time(String input) {
		if (input.matches("[0-9]{10}")) {
			return true;
		} else {
			ErrorHandler.failed(input, "Time Validity test");
			return false;
		}
	}

	public static boolean duration(String input) {
		if (input.matches("[0-9]{1,4}")) {
			return true;
		} else {
			ErrorHandler.failed(input, "Flight Duration Validity test");
			return false;
		}
	}

	public static boolean airport_name(String input) {
		if (input.matches("[A-Z]{3,20}")) {
			return true;
		} else {
			ErrorHandler.failed(input, "Airport name validity test");
			return false;
		}
	}

	public static boolean latlong(String input) {
		if (input.matches("-?\\d+\\.\\d+")) {
			return true;
		} else {
			ErrorHandler.failed(input, "Latitude and longitude validity test");
			return false;
		}
	}
}
