package util;

import java.io.StringReader;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;

/**
 * This is an example of <code>join</code>
 * <p>
 * Illustrates joining two csv files
 * <p>
 * Also uses:
 * <p>
 * - <code>CSVReader</code> for easy parsing of CSV files <br>
 * - <code>mapToPair</code> to parse each line into a key, value pair <br>
 * - <code>collect</code> to create a list of results <br>
 *
 */
public class JoinCsvFiles {
	private JavaPairRDD<String, Tuple2<String[], String[]>> results;

	public static class ParseLine implements PairFunction<String, String, String[]> {
		@Override
		public Tuple2<String, String[]> call(String line) throws Exception {
			CSVReader reader = new CSVReader(new StringReader(line));
			String[] elements = reader.readNext();
			String key = elements[0];
			return new Tuple2<String, String[]>(key, elements);
		}
	}

	public void run(JavaSparkContext sc, String master, String csv1, String csv2) throws Exception {

		JavaRDD<String> csvFile1 = sc.textFile(csv1);
		JavaRDD<String> csvFile2 = sc.textFile(csv2);

		// use the ParseLine class to parse each line
		JavaPairRDD<String, String[]> keyedRDD1 = csvFile1.mapToPair(new ParseLine());
		JavaPairRDD<String, String[]> keyedRDD2 = csvFile2.mapToPair(new ParseLine());

		// join
		results = keyedRDD1.join(keyedRDD2);

	}

	public JavaPairRDD<String, Tuple2<String[], String[]>> getResults() {
		return results;
	}
}
