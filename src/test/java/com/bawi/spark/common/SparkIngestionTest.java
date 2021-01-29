package com.bawi.spark.common;

import com.bawi.spark.*;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.json.JSONException;
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

    @Test
    public void shouldTransformLocalParallelCollectionToOutputJson() throws JSONException {
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
                .master("local[*]")
                .getOrCreate();

        // given
        SparkApp sparkApp = new SparkApp(sparkSession);

        // when
        sparkApp.run();

        // then
        String expectedSchema = getFileContent("expected/expected-schema.json");
        JSONAssert.assertEquals(expectedSchema, sparkApp.schemaAndJson.get(0), JSONCompareMode.STRICT_ORDER);
        String expectedJson = getFileContent("expected/expected-json.json");
        JSONAssert.assertEquals(expectedJson, sparkApp.schemaAndJson.get(1), JSONCompareMode.STRICT_ORDER);

        // after
        sparkSession.stop();
    }

    @Test
    public void shouldReadHiveAndWriteJsonToDisc() throws JSONException, IOException {
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

        String testDir = "target/" + getClass().getSimpleName();
        deleteIfExists(testDir);

        SparkSession sparkSession = SparkSession.builder()
                .appName(SparkIngestionTest.class.getSimpleName())
                .config("spark.sql.warehouse.dir", new File(testDir + "/warehouse").getAbsolutePath())
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        System.setProperty("derby.system.home", new File(testDir + "/metastore_db").getAbsolutePath());
        System.setProperty("derby.stream.error.file", new File(testDir + "/derby.log").getAbsolutePath());

        sparkSession.sqlContext().sql("CREATE DATABASE mydb");
        StructType schema = (StructType)DataType.fromJson(getFileContent("hive/schema.json"));
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

        Dataset<Row> dsRow = sparkSession.read().json(testDir + "/output");
        List<String> collect = dsRow.toJSON().toJavaRDD().collect();
        String json = collect.stream().collect(Collectors.joining(",", "[", "]"));
        String expectedJson = getFileContent("expected/expected-json.json");
        JSONAssert.assertEquals(expectedJson, json, JSONCompareMode.STRICT_ORDER);
        
        // after
        sparkSession.sql("DROP TABLE mydb.mytable");
        sparkSession.sql("DROP DATABASE mydb");
        sparkSession.stop();
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