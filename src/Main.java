import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

    public static void main(String[] args){

        // Spark initialisation
        System.setProperty("hadoop.home.dir", "C:\\winutils");
        SparkConf conf = new SparkConf().setAppName("My Spark Session").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Calling static function to perform a sequential vs parallel matrix multiplication test
        MatrixMultiplication.matrixMultiplicationTest(sc);

        // Calling static function to demonstrate RDDs in Spark
        Filtering.testFiltering(sc);

    }
}