package com.bawi.spark.common;

import com.bawi.spark.LocalParallelCollectionRead;
import org.apache.commons.io.FileUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.json.JSONException;
import org.junit.Ignore;
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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkIngestionTest {
    private interface JsonOutputCollect extends DataFrameWrite, ConfigurationProvider {
        List<String> schemaAndJson = new ArrayList<>();

        @Override
        default void write(Dataset<Row> ds) {
            schemaAndJson.add(ds.schema().json());
            List<String> elements = ds.toJSON().toJavaRDD().collect();
            String json = elements.stream().collect(Collectors.joining(",", "[", "]"));
            schemaAndJson.add(json);
        }
    }

    private interface HiveRead extends DataFrameRead, ConfigurationProvider {
        Logger LOGGER = LoggerFactory.getLogger(JsonDiscWrite.class);

        @Override
        default Dataset<Row> read(SparkSession sparkSession) {
            String sqlQuery = getConfiguration().getString("sql.query");
            LOGGER.info("sql.query: {}", sqlQuery);
            return sparkSession.sql(sqlQuery);
        }
    }

    private interface JsonDiscWrite extends DataFrameWrite, ConfigurationProvider {
        Logger LOGGER = LoggerFactory.getLogger(JsonDiscWrite.class);

        @Override
        default void write(Dataset<Row> ds) {
            String writePath = getConfiguration().getString("write.path");
            LOGGER.info("writePath: {}", writePath);
            ds.write().json(writePath);
        }
    }

    @Test
    public void shouldReadHiveAndWriteJson() throws JSONException, IOException {
        class SparkApp extends SparkIngestionBase implements HiveRead, JsonDiscWrite,
                LoggingInfoListenerRegistrar, ConfigurationProvider {

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
        sparkApp.start();

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

    @Test
    public void shouldTransformParallelCollectionToOutputJson() throws JSONException {
        class SparkApp extends SparkIngestionBase implements LocalParallelCollectionRead, JsonOutputCollect,
                LoggingInfoListenerRegistrar, ConfigurationProvider {

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
        sparkApp.start();

        // then
        String expectedSchema = getFileContent("expected/expected-schema.json");
        JSONAssert.assertEquals(expectedSchema, sparkApp.schemaAndJson.get(0), JSONCompareMode.STRICT_ORDER);
        String expectedJson = getFileContent("expected/expected-json.json");
        JSONAssert.assertEquals(expectedJson, sparkApp.schemaAndJson.get(1), JSONCompareMode.STRICT_ORDER);

        // after
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