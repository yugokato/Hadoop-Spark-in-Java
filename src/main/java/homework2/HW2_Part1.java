package homework2;

import java.util.Calendar;
import java.util.regex.Pattern;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple3;

/**
 * @author <Yugo Kato>
 */
public class HW2_Part1 {
	private final static String recordRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	private final static Pattern REGEX = Pattern.compile(recordRegex);
	private static int symbolIndex = 0;
	private static int dividendIndex = 4;
	private static int priceEarningsIndex = 5;
	private static Accumulator<Integer> validRecords;
	private static Accumulator<Integer> invalidRecords;

	private static String[] header = { "Symbol", "Name", "Sector", "Price", "Dividend Yield", "Price/Earnings",
			"Earnings/Share", "Book Value", "52 week low", "52 week high", "Market Cap", "EBITDA", "Price/Sales",
			"Price/Book", "SEC Filings" };

	private static boolean testing = true;

	private static Tuple3<String, String, String> cleanData(Tuple3<String, String, String> input) {
		String symbol = input._1();
		String dividend = input._2();
		String priceearnings = input._3();
		if (dividend.isEmpty()) {
			dividend = "0.0";
		}

		if (priceearnings.isEmpty()) {
			priceearnings = Float.toString(Float.NEGATIVE_INFINITY);
		}
		return new Tuple3<String, String, String>(symbol, dividend, priceearnings);
	}

	private static boolean isFloat(String s1, String s2) {
		try {
			Float.parseFloat(s1);
			Float.parseFloat(s2);
			validRecords.add(1);
			return true;
		} catch (Exception e) {
			invalidRecords.add(1);
			return false;
		}
	}

	private static boolean checkColumnSize(String[] a) {
		if (a.length == 15) {
			return true;
		} else {
			invalidRecords.add(1);
			return false;
		}
	}

	/**
	 * In main I have supplied basic information for starting the job in
	 * Eclipse.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Arguments provided:  ");
			for (String arg : args) {
				System.out.println(arg);
			}
			System.out.printf("Usage: Provide <input dir> <output dir> \n");
			System.out.printf("Example: data/companies/SP500-constituents-financials.csv output/hw2_1 \n");
			System.exit(-1);
		}

		SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.setMaster("local");
		conf.setAppName("HW2 Part 1");
		JavaSparkContext sc = new JavaSparkContext(conf);

		String outputPath = args[1] + "_" + Calendar.getInstance().getTimeInMillis();

		validRecords = sc.accumulator(0);
		invalidRecords = sc.accumulator(0);

		JavaRDD<String> lines = sc.textFile(args[0]);

		if (testing) {
			lines.cache();
			System.out.println(lines.first());
		}

		// Parse data to 3 columns
		JavaRDD<Tuple3<String, String, String>> stockInfo = lines.map(x -> REGEX.split(x))
				.filter(x -> checkColumnSize(x))
				.map(x -> new Tuple3<String, String, String>(x[symbolIndex], x[dividendIndex], x[priceEarningsIndex]))
				.filter(x -> !x._1().equals(header[0]));

		// clean data
		JavaRDD<Tuple3<String, String, String>> filteredInfo = stockInfo.map(x -> cleanData(x))
				.filter(x -> isFloat(x._2(), x._3()));

		// Sort
		JavaRDD<Tuple3<String, String, String>> sortedInfo = filteredInfo.sortBy(x -> x._1(), true, 1);

		sortedInfo.saveAsTextFile(outputPath);

		System.out.println("Valid records:  " + validRecords.value());
		System.out.println("Inalid records:  " + invalidRecords.value());

		sc.close();
	}

}
