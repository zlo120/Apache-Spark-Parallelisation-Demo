import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.DenseMatrix;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class MatrixMultiplication {
    // Size of matrices N*N
    static final int N = 1000;
    // M number of tests performed
    static final int M = 5;
    // When using showMatrices make sure not to see N to a really high number!
    static final boolean showMatrices = false;
    static Random rand = new Random();

    public static int[][] randomMatrixGenerator() {
        int[][] matrix = new int[N][N];

        // Initialising matrices
        for (int i = 0 ; i < N ; i++) {
            for(int j = 0 ; j < N ; j++) {
                matrix[i][j]=rand.nextInt(100);
            }
        }

        return matrix;
    }
    public static DenseMatrix randomDenseMatrixGenerator() {
        // Flattened matrix is a matrix represented in a single dimension array
        //   instead of a two-dimensional array.
        double[] flattenedMatrix = new double[N*N];
        for (int i = 0 ; i < N*N ; i++ ) {
            flattenedMatrix[i] = rand.nextInt(100);
        }

        // Converting flattened matrix into Spark's DenseMatrix
        // the DenseMatrix is column based, so if the flattened matrix is [1, 4, 5, 12, 11, 2, 9, 16, 19]
        //   then the DenseMatrix will look like:
        //   01 12 09
        //   04 11 16
        //   05 02 19
        return new DenseMatrix(N, N, flattenedMatrix);

    }
    public static void matrixVisualiser(int[][] matrix) {
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                System.out.print(matrix[i][j] + "\t");
            }
            System.out.print("\n");
        }
    }
    public static void matrixVisualiser(double[] matrix) {
        // ith row
        for (int i = 0; i < N; i++) {
            // jth column
            for (int j = 0; j < N; j++) {
                System.out.print((int) matrix[i+(j*N)] + "\t");
            }

            System.out.print("\n");
        }
    }
    public static long sequentialMatrixMultiplication() {

        long startTime, endTime, duration;

        int[][] A = randomMatrixGenerator();
        int[][] B = randomMatrixGenerator();
        int[][] C = new int[N][N];

        if (showMatrices) {
            System.out.println("Matrix A: ");
            matrixVisualiser(A);
            System.out.println("Matrix B: ");
            matrixVisualiser(B);
        }

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

        if (showMatrices) {
            System.out.println("\nMatrix C: ");
            matrixVisualiser(C);
        }

        return duration;
    }
    public static long parallelMatrixMultiplication() {
        long startTime, endTime, duration;

        DenseMatrix Matrix_A = randomDenseMatrixGenerator();
        DenseMatrix Matrix_B = randomDenseMatrixGenerator();

        if (showMatrices) {
            System.out.println("Matrix A: ");
            matrixVisualiser(Matrix_A.toArray());
            System.out.println("Matrix B: ");
            matrixVisualiser(Matrix_B.toArray());
        }

        // Parallel Matrix Multiplication
        startTime = System.nanoTime();

        DenseMatrix Matrix_C = Matrix_A.multiply(Matrix_B);

        endTime = System.nanoTime();
        duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.

        if (showMatrices) {
            System.out.println("\nMatrix C: ");
            matrixVisualiser(Matrix_C.toArray());
        }

        return duration;
    }
    public static long averageExecutionTime(ArrayList<Long> testResults) {
        long sum = 0;
        for (long time : testResults) {
            sum += time;
        }

        return sum / M;
    }

    public static void matrixMultiplicationTest(JavaSparkContext sc) {
        // Generating random matrices
        long averageParallelExecutionTime, averageSequentialExecutionTime;

        // Performing M number of tests
        ArrayList<Long> sequentialTestsResults = new ArrayList<>();
        for(int i = 0; i < M; i++) {
            // matrix multiplication method
            long executionTime = sequentialMatrixMultiplication();
            sequentialTestsResults.add(executionTime);
        }

        averageSequentialExecutionTime = averageExecutionTime(sequentialTestsResults);

        // Performing M number of tests
        ArrayList<Long> parallelTestsResults = new ArrayList<>();
        for(int i = 0; i < M; i++) {
            long executionTime = parallelMatrixMultiplication();
            parallelTestsResults.add(executionTime);
        }

        averageParallelExecutionTime = averageExecutionTime(parallelTestsResults);

        // Output the test results to a text file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("matrixMultiplicationOutputs/Output.txt"))) {
            // Write the content to the file
            writer.write("Average execution time of matrix multiplication " +
                    M + " tests. \n\nParallel: " + averageParallelExecutionTime +
                    " milliseconds\nSequential: " + averageSequentialExecutionTime + " milliseconds."  +
                    "\n\nSpeed up: " + ((float) averageSequentialExecutionTime / averageParallelExecutionTime));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
