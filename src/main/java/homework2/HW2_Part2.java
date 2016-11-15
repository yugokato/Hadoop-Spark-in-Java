package homework2;

import java.io.Serializable;
import java.io.StringReader;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;
import util.JoinCsvFiles;

public class HW2_Part2 {
	private static int low52wIndex = 8;
	private static int high52wIndex = 9;
	private static final String BLANK = "";

	static class MySecondarySort implements Comparator<Tuple2<String, Float>>, Serializable {
		final static MySecondarySort INSTANCE = new MySecondarySort();

		@Override
		public int compare(Tuple2<String, Float> value1, Tuple2<String, Float> value2) {
			return -value1._2().compareTo(value2._2());
		}
	}

	public static class ParseLine implements PairFunction<String, String, String[]> {
		@Override
		public Tuple2<String, String[]> call(String line) throws Exception {
			CSVReader reader = new CSVReader(new StringReader(line));
			String[] elements = reader.readNext();
			String key = elements[0];
			return new Tuple2<String, String[]>(key, elements);
		}
	}

	private static Tuple2<String, String> cleanData(Tuple2<String, String> input) {
		String low = input._1();
		String high = input._2();
		if (low.isEmpty()) {
			low = "0.0";
		}

		if (high.isEmpty()) {
			high = "0.0";
		}
		return new Tuple2<String, String>(low, high);
	}

	private static float cleanData(String input) {
		String dividend = input;
		if (dividend.isEmpty())
			dividend = "0.0";
		return Float.parseFloat(dividend);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new Exception("In Eclipse run configuration, add arguments:  <input csv file1> <input csv file2>");
		}
		String master = "local";
		JavaSparkContext sc = new JavaSparkContext(master, "basicjoincsv");
		Accumulator<Integer> counter = sc.accumulator(0);

		String csv1 = args[0];
		String csv2 = args[1];

		JoinCsvFiles jcsv = new JoinCsvFiles();
		jcsv.run(sc, master, csv1, csv2);

		// For question 1: Inner join 2 csv files
		JavaPairRDD<String, Tuple2<String[], String[]>> results = jcsv.getResults()
				.filter(x -> !x._1().equals("Symbol"));
		results.cache();

		List<Tuple2<String, Tuple2<String[], String[]>>> resultCollection = results.collect();
		for (Tuple2<String, Tuple2<String[], String[]>> result : resultCollection) {
			System.out.println(result);
		}

		// For question 2: Top 10 stocks with the greatest percentage increase
		// over a 52-week period
		JavaPairRDD<String, Tuple2<String, String>> lowhigh = results
				.mapValues(x -> new Tuple2<String, String>(x._2()[low52wIndex], x._2()[high52wIndex]));
		JavaPairRDD<String, Float> increaseRate = lowhigh.mapValues(x -> cleanData(x))
				.mapValues(x -> (Float.parseFloat(x._2()) / Float.parseFloat(x._1())) * 100);
		List<Tuple2<String, Float>> top10IncreaseRate = increaseRate.takeOrdered(10, MySecondarySort.INSTANCE);

		// For question 3: Top 10 stocks with the highest divide
		JavaPairRDD<String, Float> dividend = results.mapValues(x -> x._2()[4]).mapValues(x -> cleanData(x));
		List<Tuple2<String, Float>> top10Dividends = dividend.takeOrdered(10, MySecondarySort.INSTANCE);

		// Answer 1
		System.out.println();
		System.out.println("# How many stocks are both on the NASDAQ and in the SP500?: " + results.count());

		// Answer 2
		System.out.println();
		System.out
				.println("# What are the top 10 stocks with the greatest percentage increase over a 52-week period?:");
		System.out.println(top10IncreaseRate.toString());

		// Answer 3
		System.out.println();
		System.out.println("# What are the top 10 stocks with the highest dividends?");
		System.out.println(top10Dividends.toString());

	}
}
