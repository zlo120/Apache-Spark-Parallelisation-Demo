import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class Filtering {
    static int M = 5;
    public static long averageExecutionTime(long[] testResults) {
        long sum = 0;
        for (long time : testResults) {
            sum += time;
        }

        return sum / M;
    }
    // Method to do sequential data filtering to see which numbers are divisible by 2
    public static long parallelDataFiltering(JavaSparkContext sc, int i) {
        JavaRDD<String> lines = sc.textFile("test_data.csv");

        long startTime, endTime;

        startTime = System.nanoTime();

        JavaRDD<String> primeLines = lines.flatMap(line -> Arrays.asList(line.split(",")))
                .filter(number -> {
                    try {
                        int num = Integer.parseInt(number);
                        return isPrime(num);
                    } catch (NumberFormatException e) {
                        return false;
                    }
                });

        endTime = System.nanoTime();

        primeLines.coalesce(1).saveAsTextFile("filteringOutputs/" + i + "_prime_numbers");

        // returning the duration
        return (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
    }

    // Method to perform parallel data filtering to see which numbers are divisible by 2
    public static long sequentialDataFiltering() {
        long startTime, endTime;
        ArrayList<Integer> primeNums = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader("test_data.csv"))) {
            String line;

            long time = 0;

            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");

                for (String value : values) {
                    int num = Integer.parseInt(value.trim());

                    // Timing only the execution of isPrime()
                    // Trying to isolate it from the time it takes to read the test_data.csv file as well as
                    //   isolating it from the time it takes to write to prime_numbers.csv file
                    startTime = System.nanoTime();
                    if (isPrime(num)) {
                        primeNums.add(num);
                    }
                    endTime = System.nanoTime();

                    time += (endTime - startTime);
                }
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter("filteringOutputs/prime_numbers.csv"))) {

                // foreach num in primeNums
                for(int num : primeNums) {
                    writer.write(Integer.toString(num));
                    writer.newLine();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            return time / 1000000;  //divide by 1000000 to get milliseconds.

        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }

    }
    // Checking if a number is prime
    // Method taken from https://www.geeksforgeeks.org/prime-numbers/
    public static boolean isPrime(int n) {
        // Check if number is less than
        // equal to 1
        if (n <= 1)
            return false;

            // Check if number is 2
        else if (n == 2)
            return true;

            // Check if n is a multiple of 2
        else if (n % 2 == 0)
            return false;

        // If not, then just check the odds
        for (int i = 3; i <= Math.sqrt(n); i += 2) {
            if (n % i == 0)
                return false;
        }
        return true;
    }

    public static void testFiltering(JavaSparkContext sc) {
        long[] parallelTestResults = new long[M];
        long[] sequentialTestResults = new long[M];

        for (int i = 0; i < M; i++) {
            parallelTestResults[i] = parallelDataFiltering(sc, i);
            sequentialTestResults[i] = sequentialDataFiltering();
        }

        long averageParallelExecutionTime = averageExecutionTime(parallelTestResults);
        long averageSequentialExecutionTime = averageExecutionTime(sequentialTestResults);

        // Output the test results to a text file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("filteringOutputs/TestOutput.txt"))) {
            // Write the content to the file
            writer.write("Average execution time of filtering the test_data.csv file from " +
                    M + " tests. \n\nParallel: " + averageParallelExecutionTime +
                    " milliseconds\nSequential: " + averageSequentialExecutionTime + " milliseconds");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
