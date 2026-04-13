package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public final class HbaseTableCountUtil {

    private HbaseTableCountUtil() {
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("HbaseTableCountUtil")
                .getOrCreate();

        try {
            CountConfig config = parseArgs(args);
            long count = countRows(spark, config);
            System.out.printf(
                    "[HbaseTableCount] table=%s, method=%s, count=%d%n",
                    config.getTableName(),
                    config.getMethod(),
                    count
            );
        } finally {
            spark.stop();
        }
    }

    public static CountConfig parseArgs(String[] args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException(usage());
        }
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be provided as --key value pairs\n" + usage());
        }

        Map<String, String> kv = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            String key = args[i];
            String value = args[i + 1];
            if (!key.startsWith("--")) {
                throw new IllegalArgumentException("Unsupported argument: " + key + "\n" + usage());
            }
            kv.put(key, value);
        }

        String tableName = trimToEmpty(kv.get("--table"));
        if (tableName.isEmpty()) {
            throw new IllegalArgumentException("--table is required\n" + usage());
        }

        String method = trimToEmpty(kv.getOrDefault("--method", "auto")).toLowerCase(Locale.ROOT);
        if (!Arrays.asList("auto", "coprocessor", "spark").contains(method)) {
            throw new IllegalArgumentException("Unsupported --method: " + method);
        }

        int scanCaching = parsePositiveInt(kv.getOrDefault("--scan-caching", "1000"), "--scan-caching");

        return new CountConfig(
                tableName,
                method,
                optional(kv.get("--zk-quorum")),
                trimToEmpty(kv.getOrDefault("--zk-port", "2181")),
                trimToEmpty(kv.getOrDefault("--zk-znode-parent", "/hbase")),
                optional(kv.get("--start-row")),
                optional(kv.get("--stop-row")),
                parseFamilies(kv.get("--families")),
                scanCaching
        );
    }

    public static long countRows(SparkSession spark, CountConfig config) {
        Configuration hbaseConf = buildHBaseConf(spark, config);
        Scan scan = buildScan(config);

        switch (config.getMethod()) {
            case "coprocessor":
                return countByCoprocessor(hbaseConf, config, scan);
            case "spark":
                return countBySpark(spark, hbaseConf, config, scan);
            default:
                try {
                    long result = countByCoprocessor(hbaseConf, config, scan);
                    System.out.println("[HbaseTableCount] coprocessor count succeeded");
                    return result;
                } catch (Exception e) {
                    System.err.printf(
                            "[HbaseTableCount] coprocessor count failed, fallback to spark scan: %s%n",
                            e.getMessage()
                    );
                    return countBySpark(spark, hbaseConf, config, scan);
                }
        }
    }

    public static Scan buildScan(CountConfig config) {
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(config.getScanCaching());
        scan.readVersions(1);
        scan.setFilter(new FirstKeyOnlyFilter());

        config.getStartRow().ifPresent(row -> scan.withStartRow(Bytes.toBytes(row)));
        config.getStopRow().ifPresent(row -> scan.withStopRow(Bytes.toBytes(row)));
        for (String family : config.getFamilies()) {
            scan.addFamily(Bytes.toBytes(family));
        }
        return scan;
    }

    private static Configuration buildHBaseConf(SparkSession spark, CountConfig config) {
        Configuration sparkHadoopConf = spark.sparkContext().hadoopConfiguration();
        Configuration hbaseConf = HBaseConfiguration.create(sparkHadoopConf);
        String sparkConfZk = trimToEmpty(
                spark.sparkContext().getConf().get("spark.hadoop.hbase.zookeeper.quorum", "")
        );
        String classpathZk = trimToEmpty(hbaseConf.get("hbase.zookeeper.quorum"));

        String zkQuorum = config.getZkQuorum()
                .filter(value -> !value.isEmpty())
                .orElseGet(() -> !sparkConfZk.isEmpty() ? sparkConfZk : classpathZk);

        if (zkQuorum.isEmpty()) {
            throw new IllegalArgumentException(
                    "hbase.zookeeper.quorum is required. Set --zk-quorum or spark.hadoop.hbase.zookeeper.quorum"
            );
        }

        hbaseConf.set("hbase.zookeeper.quorum", zkQuorum);
        hbaseConf.set("hbase.zookeeper.property.clientPort", config.getZkPort());
        hbaseConf.set("zookeeper.znode.parent", config.getZkZnodeParent());
        return hbaseConf;
    }

    private static long countByCoprocessor(Configuration hbaseConf, CountConfig config, Scan scan) {
        try (AggregationClient aggregationClient = new AggregationClient(hbaseConf)) {
            return aggregationClient.rowCount(
                    TableName.valueOf(config.getTableName()),
                    new LongColumnInterpreter(),
                    scan
            );
        } catch (Throwable e) {
            throw new RuntimeException("Coprocessor count failed", e);
        }
    }

    private static long countBySpark(SparkSession spark, Configuration hbaseConf, CountConfig config, Scan scan) {
        try {
            String scanBase64 = TableMapReduceUtil.convertScanToString(scan);
            hbaseConf.set(TableInputFormat.INPUT_TABLE, config.getTableName());
            hbaseConf.set(TableInputFormat.SCAN, scanBase64);

            JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
            JavaPairRDD<ImmutableBytesWritable, Result> rdd = jsc.newAPIHadoopRDD(
                    hbaseConf,
                    TableInputFormat.class,
                    ImmutableBytesWritable.class,
                    Result.class
            );
            return rdd.count();
        } catch (IOException e) {
            throw new RuntimeException("Spark scan count failed", e);
        }
    }

    private static List<String> parseFamilies(String rawFamilies) {
        if (rawFamilies == null || rawFamilies.trim().isEmpty()) {
            return new ArrayList<>();
        }

        List<String> families = new ArrayList<>();
        for (String part : rawFamilies.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                families.add(trimmed);
            }
        }
        return families;
    }

    private static Optional<String> optional(String value) {
        String trimmed = trimToEmpty(value);
        return trimmed.isEmpty() ? Optional.empty() : Optional.of(trimmed);
    }

    private static String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    private static int parsePositiveInt(String rawValue, String fieldName) {
        try {
            int value = Integer.parseInt(trimToEmpty(rawValue));
            if (value <= 0) {
                throw new IllegalArgumentException(fieldName + " must be > 0");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(fieldName + " must be a positive integer", e);
        }
    }

    private static String usage() {
        return "Usage:\n"
                + "spark-submit --class org.example.HbaseTableCountUtil <jar> \\\n"
                + "  --table <ns:table> \\\n"
                + "  [--method auto|coprocessor|spark] \\\n"
                + "  [--zk-quorum zk1,zk2,zk3] \\\n"
                + "  [--zk-port 2181] \\\n"
                + "  [--zk-znode-parent /hbase] \\\n"
                + "  [--families cf1,cf2] \\\n"
                + "  [--start-row rowKeyStart] \\\n"
                + "  [--stop-row rowKeyStop] \\\n"
                + "  [--scan-caching 1000]\n\n"
                + "Notes:\n"
                + "- auto: prefer HBase coprocessor rowCount, fallback to Spark scan\n"
                + "- scan uses FirstKeyOnlyFilter + cacheBlocks=false for large tables\n";
    }

    public static final class CountConfig implements Serializable {
        private final String tableName;
        private final String method;
        private final Optional<String> zkQuorum;
        private final String zkPort;
        private final String zkZnodeParent;
        private final Optional<String> startRow;
        private final Optional<String> stopRow;
        private final List<String> families;
        private final int scanCaching;

        public CountConfig(
                String tableName,
                String method,
                Optional<String> zkQuorum,
                String zkPort,
                String zkZnodeParent,
                Optional<String> startRow,
                Optional<String> stopRow,
                List<String> families,
                int scanCaching
        ) {
            this.tableName = tableName;
            this.method = method;
            this.zkQuorum = zkQuorum == null ? Optional.empty() : zkQuorum;
            this.zkPort = zkPort;
            this.zkZnodeParent = zkZnodeParent;
            this.startRow = startRow == null ? Optional.empty() : startRow;
            this.stopRow = stopRow == null ? Optional.empty() : stopRow;
            this.families = families == null ? new ArrayList<>() : new ArrayList<>(families);
            this.scanCaching = scanCaching;
        }

        public String getTableName() {
            return tableName;
        }

        public String getMethod() {
            return method;
        }

        public Optional<String> getZkQuorum() {
            return zkQuorum;
        }

        public String getZkPort() {
            return zkPort;
        }

        public String getZkZnodeParent() {
            return zkZnodeParent;
        }

        public Optional<String> getStartRow() {
            return startRow;
        }

        public Optional<String> getStopRow() {
            return stopRow;
        }

        public List<String> getFamilies() {
            return new ArrayList<>(families);
        }

        public int getScanCaching() {
            return scanCaching;
        }
    }
}
