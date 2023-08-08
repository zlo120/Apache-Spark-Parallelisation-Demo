import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.DenseMatrix;

import java.util.*;

public class Main {
    // Size of matrices N*N
    static final int N = 1000;
    // M number of tests performed
    static final int M = 10;

    public static double[] convertToOneDimensionalArray(int[][] twoDimensionalArray) {
        double[] oneDimensionalArray = new double[N*N];

        for (int i = 0 ; i < N*N ; i++) {
            int columnNum = i / N;
            int rowNum = i - columnNum * N;

            oneDimensionalArray[i] = twoDimensionalArray[rowNum][columnNum];
        }

        return oneDimensionalArray;
    }

    public static long sequentialMatrixMultiplication(int[][]A, int[][]B) {

        long startTime, endTime, duration;
        int[][] C = new int[N][N];

        // Sequential Matrix Multiplication
        startTime = System.nanoTime();

        for (int i = 0 ; i < N ; i++) {
            for (int j = 0 ; j < N ; j++) {
                C[i][j] = 0;
                for (int k = 0 ; k < N ; k++) {
                    C[i][j] = C[i][j] + A[i][k] * B[k][j];
                }
            }
        }

        endTime = System.nanoTime();
        duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
//        System.out.println("SEQUENTIAL MATRIX MULTIPLICATION - Execute time was: " + duration + " milliseconds");

        return duration;
    }

    public static long parallelMatrixMultiplication(DenseMatrix Matrix_A, DenseMatrix Matrix_B) {
        long startTime, endTime, duration;

        // Parallel Matrix Multiplication
        startTime = System.nanoTime();

        DenseMatrix Matrix_C = Matrix_A.multiply(Matrix_B);

        endTime = System.nanoTime();
        duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
//        System.out.println("PARALLEL MATRIX MULTIPLICATION - Execute time was: " + duration + " milliseconds");

        return duration;
    }

    public static long averageExecutionTime(ArrayList<Long> testResults) {
        long sum = 0;
        for (long time : testResults) {
            sum += time;
        }

        return sum / M;
    }

    public static void main(String[] args){

        // Spark initialisation
        System.setProperty("hadoop.home.dir", "C:\\winutils");
        SparkConf conf = new SparkConf().setAppName("Matrix Multiply").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Generating random matrices
        Random rand = new Random();

        int[][] A = new int[N][N];
        int[][] B = new int[N][N];
        int[][] C = new int[N][N];

        long averageExecutionTime;

        // Initialising matrices
        for (int i = 0 ; i < N ; i++) {
            for(int j = 0 ; j < N ; j++) {
                A[i][j]=rand.nextInt(100);
                B[i][j]=rand.nextInt(100);
            }
        }

        // Performing M number of tests
        ArrayList<Long> sequentialTestsResults = new ArrayList<>();
        for(int i = 0; i < M; i++) {
            sequentialTestsResults.add(sequentialMatrixMultiplication(A, B));
        }

        averageExecutionTime = averageExecutionTime(sequentialTestsResults);
        System.out.println("The average execution time for the SEQUENTIAL matrix multiplication was: " + averageExecutionTime + " milliseconds.");

        // Converting 2-Dimensional arrays into a flattened 1-Dimensional array
        double[] Flattened_MatrixA = convertToOneDimensionalArray(A);
        double[] Flattened_MatrixB = convertToOneDimensionalArray(B);

        // Converting flattened matrices into Spark's DenseMatrix
        DenseMatrix Matrix_A = new DenseMatrix(N, N, Flattened_MatrixA);
        DenseMatrix Matrix_B = new DenseMatrix(N, N, Flattened_MatrixB);

        // Performing M number of tests
        ArrayList<Long> parallelTestsResults = new ArrayList<>();
        for(int i = 0; i < M; i++) {
            parallelTestsResults.add(parallelMatrixMultiplication(Matrix_A, Matrix_B));
        }

        averageExecutionTime = averageExecutionTime(parallelTestsResults);
        System.out.println("The average execution time for the PARALLEL matrix multiplication was: " + averageExecutionTime + " milliseconds.");
    }
}