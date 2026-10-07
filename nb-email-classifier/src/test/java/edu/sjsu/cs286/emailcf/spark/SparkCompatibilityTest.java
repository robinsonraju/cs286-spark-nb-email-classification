package edu.sjsu.cs286.emailcf.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.junit.Test;
import scala.Tuple2;
import static org.junit.Assert.*;

public class SparkCompatibilityTest {
    @Test
    public void flatMapReturnsWordsUsingSparkIteratorContract() throws Exception {
        Iterator<String> words = new SplitWordsFunction().call("privacy data privacy");
        assertEquals("privacy", words.next());
        assertEquals("data", words.next());
        assertEquals("privacy", words.next());
        assertFalse(words.hasNext());
    }

    @Test
    public void patchedSparkCountsWordsAndJoinsAbsentKeys() throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[1]")
            .setAppName("dependency-compatibility-test")
            .set("spark.ui.enabled", "false")
            .set("spark.driver.host", "127.0.0.1")
            .set("spark.driver.bindAddress", "127.0.0.1");
        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            context.setLogLevel("ERROR");
            Map<String, Integer> counts = SparkUtil.countWords(
                context.parallelize(Arrays.asList("privacy data", "privacy"))).collectAsMap();
            assertEquals(Integer.valueOf(2), counts.get("privacy"));
            assertEquals(Integer.valueOf(1), counts.get("data"));
            Map<String, Tuple2<Optional<Integer>, Optional<Integer>>> joined =
                context.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("privacy", 2)))
                .fullOuterJoin(context.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("data", 1))))
                .collectAsMap();
            assertEquals(Integer.valueOf(2), joined.get("privacy")._1().get());
            assertFalse(joined.get("privacy")._2().isPresent());
            assertFalse(joined.get("data")._1().isPresent());
            assertEquals(Integer.valueOf(1), joined.get("data")._2().get());
        }
    }
}
