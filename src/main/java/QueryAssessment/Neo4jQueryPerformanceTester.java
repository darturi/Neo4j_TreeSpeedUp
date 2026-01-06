package QueryAssessment;

import org.neo4j.driver.*;
import org.neo4j.driver.summary.ResultSummary;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jQueryPerformanceTester {

    private final Driver driver;
    private final String uri;
    private final String username;
    private final String password;
    private final String database;

    public Neo4jQueryPerformanceTester(String uri, String username, String password) {
        this(uri, username, password, "neo4j");
    }

    public Neo4jQueryPerformanceTester(String uri, String username, String password, String database) {
        this.uri = uri;
        this.username = username;
        this.password = password;
        this.database = database;
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
    }

    public static class QueryTuple {
        public final String query;
        public final String description;

        public QueryTuple(String query, String description) {
            this.query = query;
            this.description = description;
        }
    }

    public static class PerformanceMetrics {
        public double meanTime;
        public double stdDevTime;
        public double meanDbHits;
        public double stdDevDbHits;
        public double meanMemory;
        public double stdDevMemory;

        @Override
        public String toString() {
            return String.format(
                    "Time: %.2f ms (±%.2f)\nDB Hits: %.2f (±%.2f)\nMemory: %.2f bytes (±%.2f)",
                    meanTime, stdDevTime, meanDbHits, stdDevDbHits, meanMemory, stdDevMemory
            );
        }
    }

    private static class RunMetrics {
        long time;
        long dbHits;
        long memory;
    }

    public void testQueryPerformance(String cypherQuery, int n, Map<String, Object> parameters) {
        System.out.println("=".repeat(80));
        System.out.println("Neo4j Query Performance Test");
        System.out.println("=".repeat(80));
        System.out.println("Query: " + cypherQuery);
        System.out.println("Number of runs per phase: " + n);
        System.out.println();

        // Phase 1: Cold Cache
        System.out.println("PHASE 1: COLD CACHE");
        System.out.println("-".repeat(80));
        PerformanceMetrics coldMetrics = runColdCacheTest(cypherQuery, n, parameters);
        System.out.println(coldMetrics);
        System.out.println();

        // Phase 2: Hot Cache
        System.out.println("PHASE 2: HOT CACHE");
        System.out.println("-".repeat(80));
        PerformanceMetrics hotMetrics = runHotCacheTest(cypherQuery, n, parameters);
        System.out.println(hotMetrics);
        System.out.println();

        System.out.println("=".repeat(80));
    }

    public void benchmarkQueries(List<QueryTuple> queries, String filename, int n, Map<String, Object> parameters) {
        System.out.println("=".repeat(80));
        System.out.println("Neo4j Query Benchmark Suite");
        System.out.println("=".repeat(80));
        System.out.println("Number of queries: " + queries.size());
        System.out.println("Runs per query per phase: " + n);
        System.out.println("Output file: " + filename);
        System.out.println();

        java.io.File file = new java.io.File(filename);
        java.io.File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            boolean created = parentDir.mkdirs();
            if (created) {
                System.out.println("Created directory: " + parentDir.getAbsolutePath());
            }
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            // Write CSV header
            writer.println("id,X,Y,cold_cache_time,cold_cache_total_db_accesses,cold_cache_total_allocated_memory,hot_cache_time,hot_cache_total_db_accesses,hot_cache_total_allocated_memory");

            // Process each query
            for (int i = 0; i < queries.size(); i++) {
                QueryTuple queryTuple = queries.get(i);
                int id = i + 1;

                System.out.println("-".repeat(80));
                System.out.printf("Query %d/%d: %s%n", id, queries.size(), queryTuple.description);
                System.out.println("-".repeat(80));

                // Run cold cache test
                System.out.println("Running cold cache tests...");
                PerformanceMetrics coldMetrics = runColdCacheTest(queryTuple.query, n, parameters);

                // Run hot cache test
                System.out.println("Running hot cache tests...");
                PerformanceMetrics hotMetrics = runHotCacheTest(queryTuple.query, n, parameters);

                // Write results to CSV
                writer.printf("%d,\"%s\",\"%s\",%.2f,%.2f,%.2f,%.2f,%.2f,%.2f%n",
                        id,
                        escapeCSV(queryTuple.query),
                        escapeCSV(queryTuple.description),
                        coldMetrics.meanTime,
                        coldMetrics.meanDbHits,
                        coldMetrics.meanMemory,
                        hotMetrics.meanTime,
                        hotMetrics.meanDbHits,
                        hotMetrics.meanMemory
                );

                // Display summary
                System.out.printf("Cold Cache - Time: %.2f ms, DB Hits: %.2f, Memory: %.2f bytes%n",
                        coldMetrics.meanTime, coldMetrics.meanDbHits, coldMetrics.meanMemory);
                System.out.printf("Hot Cache  - Time: %.2f ms, DB Hits: %.2f, Memory: %.2f bytes%n",
                        hotMetrics.meanTime, hotMetrics.meanDbHits, hotMetrics.meanMemory);
                System.out.println();
            }

            System.out.println("=".repeat(80));
            System.out.println("Benchmark complete! Results written to: " + filename);
            System.out.println("=".repeat(80));

        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String escapeCSV(String value) {
        if (value == null) {
            return "";
        }
        // Escape quotes by doubling them
        return value.replace("\"", "\"\"");
    }

    private PerformanceMetrics runColdCacheTest(String cypherQuery, int n, Map<String, Object> parameters) {
        List<RunMetrics> results = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            clearCaches();
            RunMetrics metrics = executeProfiledQuery(cypherQuery, parameters);
            results.add(metrics);
            System.out.printf("Run %d/%d completed%n", i + 1, n);
        }

        return calculateStatistics(results);
    }

    private PerformanceMetrics runHotCacheTest(String cypherQuery, int n, Map<String, Object> parameters) {
        // Warm up the cache
        System.out.println("Warming up cache...");
        int warmupRuns = Math.max(5, n / 2);
        for (int i = 0; i < warmupRuns; i++) {
            executeProfiledQuery(cypherQuery, parameters);
        }
        System.out.println("Cache warmed. Starting measurements...");

        List<RunMetrics> results = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            RunMetrics metrics = executeProfiledQuery(cypherQuery, parameters);
            results.add(metrics);
            System.out.printf("Run %d/%d completed%n", i + 1, n);
        }

        return calculateStatistics(results);
    }

    private RunMetrics executeProfiledQuery(String cypherQuery, Map<String, Object> parameters) {
        String profileQuery = "PROFILE " + cypherQuery;
        RunMetrics metrics = new RunMetrics();

        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            long startTime = System.nanoTime();

            Result result = session.run(profileQuery, parameters);

            // Consume all results
            while (result.hasNext()) {
                result.next();
            }

            long endTime = System.nanoTime();
            metrics.time = (endTime - startTime) / 1_000_000; // Convert to milliseconds

            // Extract profile information
            ResultSummary summary = result.consume();
            if (summary.hasPlan() && summary.hasProfile()) {
                metrics.dbHits = extractDbHits(summary.profile());

                // Try to get GlobalMemory first (total for entire query)
                Object globalMem = summary.profile().arguments().get("GlobalMemory");
                if (globalMem != null) {
                    metrics.memory = parseMemoryValue(globalMem);
                } else {
                    // Fallback: sum memory from all operators
                    metrics.memory = extractMemory(summary.profile());
                }
            }
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
            e.printStackTrace();
        }

        return metrics;
    }

    private long parseMemoryValue(Object memValue) {
        if (memValue instanceof Number) {
            return ((Number) memValue).longValue();
        } else if (memValue instanceof org.neo4j.driver.Value) {
            // Handle Neo4j Value types (IntegerValue, etc.)
            org.neo4j.driver.Value value = (org.neo4j.driver.Value) memValue;
            if (!value.isNull()) {
                return value.asLong();
            }
        } else if (memValue instanceof String) {
            // Try to parse numeric value from string
            try {
                return Long.parseLong(memValue.toString().replaceAll("[^0-9]", ""));
            } catch (NumberFormatException e) {
                return 0;
            }
        } else if (memValue instanceof Map) {
            // If it's a map, look for common keys
            @SuppressWarnings("unchecked")
            Map<String, Object> memMap = (Map<String, Object>) memValue;
            for (String key : new String[]{"value", "bytes", "amount", "total"}) {
                if (memMap.containsKey(key)) {
                    return parseMemoryValue(memMap.get(key));
                }
            }
        }
        return 0;
    }

    private long extractDbHits(org.neo4j.driver.summary.ProfiledPlan profile) {
        long total = profile.dbHits();
        for (org.neo4j.driver.summary.ProfiledPlan child : profile.children()) {
            total += extractDbHits(child);
        }
        return total;
    }

    private long extractMemory(org.neo4j.driver.summary.ProfiledPlan profile) {
        long total = 0;

        // Check Memory field in current operator
        Object memValue = profile.arguments().get("Memory");
        if (memValue != null) {
            total += parseMemoryValue(memValue);
        }

        // Recursively sum from children
        for (org.neo4j.driver.summary.ProfiledPlan child : profile.children()) {
            total += extractMemory(child);
        }

        return total;
    }

    private void clearCaches() {
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            // Clear page cache
            session.run("CALL db.clearQueryCaches()").consume();

            // Add a small delay to ensure cache is cleared
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } catch (Exception e) {
            System.err.println("Warning: Could not clear caches: " + e.getMessage());
        }
    }

    private PerformanceMetrics calculateStatistics(List<RunMetrics> results) {
        PerformanceMetrics metrics = new PerformanceMetrics();

        // Calculate means
        double sumTime = 0, sumDbHits = 0, sumMemory = 0;
        for (RunMetrics r : results) {
            sumTime += r.time;
            sumDbHits += r.dbHits;
            sumMemory += r.memory;
        }

        int n = results.size();
        metrics.meanTime = sumTime / n;
        metrics.meanDbHits = sumDbHits / n;
        metrics.meanMemory = sumMemory / n;

        // Calculate standard deviations
        double sumSqDiffTime = 0, sumSqDiffDbHits = 0, sumSqDiffMemory = 0;
        for (RunMetrics r : results) {
            sumSqDiffTime += Math.pow(r.time - metrics.meanTime, 2);
            sumSqDiffDbHits += Math.pow(r.dbHits - metrics.meanDbHits, 2);
            sumSqDiffMemory += Math.pow(r.memory - metrics.meanMemory, 2);
        }

        metrics.stdDevTime = Math.sqrt(sumSqDiffTime / n);
        metrics.stdDevDbHits = Math.sqrt(sumSqDiffDbHits / n);
        metrics.stdDevMemory = Math.sqrt(sumSqDiffMemory / n);

        return metrics;
    }

    public void close() {
        driver.close();
    }

    public static void run_test(String database, Map<String, Object> parameters, String save_dir){
        String uri = "bolt://localhost:7687";
        String username = "neo4j";
        String password = "gigglerhombus";

        Neo4jQueryPerformanceTester tester = new Neo4jQueryPerformanceTester(uri, username, password, database);

        try {
            int numberOfRuns = 10;

            // Run vanilla queries with parameters
            System.out.println("Starting vanilla queries benchmark...\n");
            tester.benchmarkQueries(
                    GetQueryList.getFakeTreeTraversalQueries(),
                    save_dir + "/vanilla_results_" + database + ".csv",
                    numberOfRuns,
                    parameters
            );

            // Run index-optimized queries with parameters
            System.out.println("\nStarting index-optimized queries benchmark...\n");
            tester.benchmarkQueries(
                    GetQueryList.getFakeIndexOptimizedQueries(),
                    save_dir + "/index_results_" + database + ".csv",
                    numberOfRuns,
                    parameters
            );

        } finally {
            tester.close();
        }
    }

    public static void run_test_battery(){
        String save_dir = "src/main/save_dir";

        // 10
        String database10 = "float.10";
        Map<String, Object> parameters10 = new HashMap<>();
        parameters10.put("root_id", 1);
        parameters10.put("leaf_id", 1);
        parameters10.put("node_id", 9);
        run_test(database10, parameters10, save_dir);

        // 100
        String database100 = "float.100";
        Map<String, Object> parameters100 = new HashMap<>();
        parameters100.put("root_id", 1);
        parameters100.put("leaf_id", 98);
        parameters100.put("node_id", 9);
        run_test(database100, parameters100, save_dir);

        // 1k
        String database1k = "float.1k";
        Map<String, Object> parameters1k = new HashMap<>();
        parameters1k.put("root_id", 1);
        parameters1k.put("leaf_id", 736);
        parameters1k.put("node_id", 83);
        run_test(database1k, parameters1k, save_dir);

        // 10k
        String database10k = "float.10k";
        Map<String, Object> parameters10k = new HashMap<>();
        parameters10k.put("root_id", 1);
        parameters10k.put("leaf_id", 6955);
        parameters10k.put("node_id", 77);
        run_test(database10k, parameters10k, save_dir);

        // 100k
        String database100k = "float.100k";
        Map<String, Object> parameters100k = new HashMap<>();
        parameters100k.put("root_id", 1);
        parameters100k.put("leaf_id", 12202);
        parameters100k.put("node_id", 3);
        run_test(database100k, parameters100k, save_dir);
    }

    public static void main(String[] args) {
        run_test_battery();
    }
}
