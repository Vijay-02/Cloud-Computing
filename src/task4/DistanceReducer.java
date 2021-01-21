package task4;


import com.mapreduce.core.KeyValPair;
import com.mapreduce.core.Reducer;
import com.mapreduce.util.ErrorHandler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class DistanceReducer extends Reducer {
	@Override
	protected KeyValPair reduce(String key, ArrayList<String> values) {

		Set<String> uniqueValues = new HashSet<>(values);
		int milesTotal = 0;

		for (String combo : uniqueValues) {

			String[] airports = combo.split("\\|");

			String fAirportCode = airports[0];
			String dAirportCode = airports[1];

			float[] FApos = new float[2];
			float[] DApos = new float[2];

			try (BufferedReader br = new BufferedReader(new FileReader("Top30_airports_LatLong.csv"))) {
				String line;
				while ((line = br.readLine()) != null) {
					String[] split = line.split(",");
					if (!split[0].equals(line)) {
						String code = split[1];
						if (ErrorHandler.airport(code)) {
							String lat = split[2];
							String lng = split[3];

							if (ErrorHandler.latlong(lat) && ErrorHandler.latlong(lng)) {
								if (code.equals(fAirportCode)) {
									FApos[0] = Float.parseFloat(lat);
									FApos[1] = Float.parseFloat(lng);
								} else if (code.equals(dAirportCode)) {
									DApos[0] = Float.parseFloat(lat);
									DApos[1] = Float.parseFloat(lng);
								}
							}

						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			double metres = this.latLongToMetres(FApos[0], FApos[1], DApos[0], DApos[1]);

			int miles = (int) metres / 1852;

			milesTotal += miles;
		}

		return new KeyValPair(key, String.valueOf(milesTotal));
	}

	/**
	 *
	 *
	 * @param fLat The latitude of the from airport
	 * @param fLon The longitude  of the from airport.
	 * @param dLat The latitude of the destination airport.
	 * @param dLon The longitude of destination airport.
	 * @return The distance between the coordinates in metres.
	 */
	private double latLongToMetres(float fLat, float fLon, float dLat, float dLon) {
		double earthRadius = 6378.137; // Radius of earth in KM
		double distanceLat = dLat * Math.PI / 180 - fLat * Math.PI / 180;
		double distanceLon = dLon * Math.PI / 180 - fLon * Math.PI / 180;
		double a = Math.sin(distanceLat / 2) * Math.sin(distanceLat / 2) +
				Math.cos(fLat * Math.PI / 180) * Math.cos(dLat * Math.PI / 180) *
						Math.sin(distanceLon / 2) * Math.sin(distanceLon / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double d = earthRadius * c;
		return d * 1000; // meters
	}
}
