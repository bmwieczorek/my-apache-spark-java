package com.bawi.spark.common;

import com.bawi.spark.HiveRead;
import com.bawi.spark.JsonDiscWrite;
import com.bawi.spark.LocalParallelCollectionRead;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SparkIngestionTest {
    private interface JsonOutputLoggerWrite extends DataFrameWrite, ConfigurationProvider {
        Logger LOGGER = LoggerFactory.getLogger(JsonOutputLoggerWrite.class);
        List<String> schemaAndJson = new ArrayList<>();

        @Override
        default void write(Dataset<Row> ds) {
            String schema = ds.schema().json();
            schemaAndJson.add(schema);
            LOGGER.info("schema: {}", schema);
            List<String> elements = ds.toJSON().toJavaRDD().collect();
            String json = elements.stream().collect(Collectors.joining(",", "[", "]"));
            schemaAndJson.add(json);
            LOGGER.info("json: {}", json);
        }
    }

    private static final String TEST_DIR = "target/" + SparkIngestionTest.class.getSimpleName();

    @Before
    public void before() throws IOException {
        deleteIfExists(TEST_DIR);
    }

    @Test
    public void shouldTransformLocalParallelCollectionToOutputJson() throws JSONException {

        // 2 record processed, 0 records read from disc, 0 records written to disc
        class SparkApp extends SparkReadWriteBase implements LocalParallelCollectionRead, JsonOutputLoggerWrite, ConfigurationProvider {

            public SparkApp(SparkSession sparkSession) {
                super(sparkSession);
            }

            @Override
            public Configuration getConfiguration() {
                return new Configuration(new Properties());
            }
        }

        // before
        SparkSession sparkSession = SparkSession.builder()
                .appName(SparkIngestionTest.class.getSimpleName())
                .enableHiveSupport()
                .master("local[*]")
                .getOrCreate();

        // given
        SparkApp sparkApp = new SparkApp(sparkSession);

        // when
        sparkApp.run();

        // then
        String expectedSchema = getFileContent("expected/expected-schema.json");
        JSONAssert.assertEquals(expectedSchema, JsonOutputLoggerWrite.schemaAndJson.get(0), JSONCompareMode.STRICT_ORDER);
        String expectedJson = getFileContent("expected/expected-json.json");
        JSONAssert.assertEquals(expectedJson, JsonOutputLoggerWrite.schemaAndJson.get(1), JSONCompareMode.STRICT_ORDER);

        // after
        sparkSession.stop();
    }

    @Test
    public void shouldReadHiveAndWriteJsonToDisc() throws JSONException {
        class SparkApp extends SparkReadWriteBase implements HiveRead, JsonDiscWrite,
                LoggingInfoListenerRegistrar, CustomSparkMetricsRegistrar, ConfigurationProvider {

            public SparkApp(SparkSession sparkSession) {
                super(sparkSession);
            }

            @Override
            public Configuration getConfiguration() {
                return new Configuration("app.properties");
            }
        }

        SparkSession sparkSession = SparkSession.builder()
                .appName(SparkIngestionTest.class.getSimpleName())
                .config("spark.sql.warehouse.dir", new File(TEST_DIR + "/warehouse").getAbsolutePath())
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        System.setProperty("derby.system.home", new File(TEST_DIR + "/metastore_db").getAbsolutePath());
        System.setProperty("derby.stream.error.file", new File(TEST_DIR + "/derby.log").getAbsolutePath());

        sparkSession.sqlContext().sql("CREATE DATABASE mydb");
        StructType schema = (StructType) DataType.fromJson(getFileContent("hive/schema.json"));
        Path jsonDirPath = getPath("hive/data");

        String absolutePath = jsonDirPath.toAbsolutePath().toString();
        Dataset<Row> ds = sparkSession.sqlContext().read()
                .schema(schema)
                .json(absolutePath);
        ds.write().saveAsTable("mydb.mytable");

        // given
        SparkApp sparkApp = new SparkApp(sparkSession);

        // when
        sparkApp.run();

        Dataset<Row> dsRow = sparkSession.read().json(TEST_DIR + "/output");
        List<String> collect = dsRow.toJSON().toJavaRDD().collect();
        String json = collect.stream().collect(Collectors.joining(",", "[", "]"));
        String expectedJson = getFileContent("expected/expected-json.json");
        JSONAssert.assertEquals(expectedJson, json, JSONCompareMode.STRICT_ORDER);

        // after
        sparkSession.sql("DROP TABLE mydb.mytable");
        sparkSession.sql("DROP DATABASE mydb");
        sparkSession.stop();
    }

    @Test(expected = SparkException.class)
    public void shouldFail() {
        Pattern DIGITS_PATTEN = Pattern.compile("[0-9]+");

        class SparkApp extends SparkBase implements CustomSparkMetricsRegistrar {
            SparkApp(SparkSession sparkSession) {
                super(sparkSession);
            }

            @Override
            protected void doInRun(SparkSession sparkSession) {
                // requires spark-avro_2.11 dependency
                Dataset<Row> dsRow = sparkSession.read().format("avro")
                        .load("src/test/resources/avro/numbers-1-2-a-4.avro");
                Dataset<String> dsString = dsRow.as(Encoders.STRING());
                MapFunction<String, Integer> func = Integer::parseInt; // extract type to avoid ambiguous method call (other scala not @Functional interface)
                Dataset<Integer> dsInt = dsString
                        //.filter((FilterFunction<String>) s -> DIGITS_PATTEN.matcher(s).matches())
                        .map(func, Encoders.INT());
                dsInt.write().format("avro").save(TEST_DIR + "/avro");
            }
        }
        SparkSession sparkSession = SparkSession.builder()
                .appName(SparkIngestionTest.class.getSimpleName())
                .master("local[1]")
                .getOrCreate();

        new SparkApp(sparkSession).run();

    }

    private static String getFileContent(String relativeFilePath) {
        Path path = getPath(relativeFilePath);
        return getFileContent(path);
    }

    private static String getFileContent(Path path) {
        try {
            return new String(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read: " + path + " due to: " + e);
        }
    }

    private static Path getPath(String relativePath) {
        try {
            URL resource = SparkIngestionTest.class.getClassLoader().getResource(relativePath);
            if (resource == null) {
                throw new IllegalArgumentException("Path does not exist: " + relativePath);
            }
            return Paths.get(resource.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Failed to read: " + relativePath + " due to: " + e);
        }
    }

    private void deleteIfExists(String path) throws IOException {
        File file = new File(path);
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
    }
}