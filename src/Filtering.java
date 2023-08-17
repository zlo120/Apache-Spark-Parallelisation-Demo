import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class Filtering {
    static int M = 10;
    public static long averageExecutionTime(long[] testResults) {
        long sum = 0;
        for (long time : testResults) {
            sum += time;
        }

        return sum / M;
    }
    // Method to do sequential data filtering to see which numbers are divisible by 2
    public static long parallelDataFiltering(JavaSparkContext sc) {
        JavaRDD<String> lines = sc.textFile("test_data.csv");

        long startTime, endTime;

        startTime = System.nanoTime();

        JavaRDD<String> divisibleBy2 = lines
                .flatMap(line -> Arrays.asList(line.split(",")))
                .filter(number -> {
                    try {
                        int num = Integer.parseInt(number);
                        return num % 2 == 0;
                    } catch (NumberFormatException e) {
                        return false;
                    }
                });

//        divisibleBy2.saveAsTextFile("divisible_by_2");

        endTime = System.nanoTime();

        // returning the duration
        return (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
    }

    // Method to perform parallel data filtering to see which numbers are divisible by 2
    public static long sequentialDataFiltering() {
        long startTime, endTime;

        startTime = System.nanoTime();

        try (BufferedReader reader = new BufferedReader(new FileReader("test_data.csv"));
             BufferedWriter writer = new BufferedWriter(new FileWriter("divisible_by_2.csv"))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                for (String value : values) {
                    int number = Integer.parseInt(value.trim());
                    if (number % 2 == 0) {
                        writer.write(Integer.toString(number));
                        writer.newLine();
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        endTime = System.nanoTime();

        return (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
    }

    public static void testFiltering(JavaSparkContext sc) {
        long[] parallelTestResults = new long[M];
        long[] sequentialTestResults = new long[M];

        for (int i = 0; i < M; i++) {
            parallelTestResults[i] = parallelDataFiltering(sc);
            sequentialTestResults[i] = sequentialDataFiltering();
        }

        long averageParallelExecutionTime = averageExecutionTime(parallelTestResults);
        long averageSequentialExecutionTime = averageExecutionTime(sequentialTestResults);

        // Output the test results to a text file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("TestOutput.txt"))) {
            // Write the content to the file
            writer.write("This is the average execution time of filtering the test_data.csv file, this test finds " +
                    "the average execution time in milliseconds of " + M + " tests. \nParallel: " + averageParallelExecutionTime +
                    " milliseconds\nSequential: " + averageSequentialExecutionTime + " milliseconds.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
