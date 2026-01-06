package QueryAssessment2;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.ResultSummary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;

import static QueryAssessment2.CSVProcessor.csv_to_hashmap;

public class QueryAssessment {
    private final String uri;
    private final String username;
    private final String password;
    private HashMap<String, HashMap<String, String>> q_df;
    private final Driver driver;
    private HashMap<Integer, String> size_map;

    private Random rng;
    public QueryAssessment(String uri, String username, String password, Long seed, String csv_path, HashMap<Integer, String> size_map){
        this.uri = uri;
        this.username = username;
        this.password = password;

        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));

        this.rng = new Random(seed);

        this.q_df = csv_to_hashmap(csv_path);

        this.size_map = size_map;

        //for (Map.Entry<String, HashMap<String, String>> entry : this.q_df.entrySet()) {
        //    System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
        //}
    }

    private HashMap<String, String> generate_integer_range_mapping(int int_size){
        HashMap<String, String> r_val = new HashMap<>();

        r_val.put("$rootID", "1");
        r_val.put("$id1", Integer.toString(this.rng.nextInt(int_size - 1) + 2));
        r_val.put("$id2", Integer.toString(this.rng.nextInt(int_size - 1) + 2));
        r_val.put("$nodeID", Integer.toString(this.rng.nextInt(int_size - 1) + 2));
        r_val.put("**", ",");

        return r_val;
    }

    private HashMap<String, String> generate_string_mapping(int int_size, Session session){
        HashMap<String, String> r_val = new HashMap<>();

        int r1 = this.rng.nextInt(int_size - 1) + 2;
        int r2 = this.rng.nextInt(int_size - 1) + 2;
        int r3 = this.rng.nextInt(int_size - 1) + 2;

        r_val.put("$rootID", "'1'");
        r_val.put("$id1", "'" + this.getStringIdByName(r1, session) + "'");
        r_val.put("$id2", "'" + this.getStringIdByName(r2, session) + "'");
        r_val.put("$nodeID", "'" + this.getStringIdByName(r3, session) + "'");
        r_val.put("**", ",");

        return r_val;
    }

    private ArrayList<HashMap<String, String>> generate_integer_range_mapping_list(int count, int int_size, String type, Session session){
        ArrayList<HashMap<String, String>> r_val = new ArrayList<>();

        if (type.equals("int")) {
            for (int i = 0; i < count; i++)
                r_val.add(this.generate_integer_range_mapping(int_size));
        } else if (type.equals("string")) {
            for (int i = 0; i < count; i++)
                r_val.add(this.generate_string_mapping(int_size, session));
        }


        return r_val;
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

    private long extractDbHits(org.neo4j.driver.summary.ProfiledPlan profile) {
        long total = profile.dbHits();
        for (org.neo4j.driver.summary.ProfiledPlan child : profile.children()) {
            total += extractDbHits(child);
        }
        return total;
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

    private HashMap<String, Double> assess_single_query(String query, HashMap<String, String> query_params, Session session){
        // Map keys into the right place
        String processed_query = query;
        for (Map.Entry<String, String> entry : query_params.entrySet())
            processed_query = processed_query.replace(entry.getKey(), entry.getValue());

        HashMap<String, Double> metrics = new HashMap<>();

        String profileQuery = "PROFILE ".concat(processed_query);

        // System.out.println("HELLO IMPORTANT");
        // System.out.println(processed_query);
        // System.out.println(profileQuery);

        long startTime = System.nanoTime();

        Result result = session.run(profileQuery);

        // Consume all results
        while (result.hasNext()) {
                result.next();
            }

        long endTime = System.nanoTime();
        double timeMs = (endTime - startTime) / 1_000_000.0; // Convert to milliseconds
        metrics.put("time", timeMs);

        // Extract profile information
        ResultSummary summary = result.consume();
        if (summary.hasPlan() && summary.hasProfile()) {
            double dbHits = (double) extractDbHits(summary.profile());
            metrics.put("dbHits", dbHits);

            // Try to get GlobalMemory first (total for entire query)
            Object globalMem = summary.profile().arguments().get("GlobalMemory");
            double memory;
            if (globalMem != null) {
                memory = (double) parseMemoryValue(globalMem);
            } else {
                // Fallback: sum memory from all operators
                memory = (double) extractMemory(summary.profile());
            }
            metrics.put("memory", memory);
        } else {
            // If profile info not available, set to 0
            metrics.put("dbHits", 0.0);
            metrics.put("memory", 0.0);
        }


        return metrics;
    }

    private HashMap<String, Double> assess_query_averaged(
            String query,
            ArrayList<HashMap<String, String>> paramsList,
            Session session
    ) {
        if (paramsList == null || paramsList.isEmpty()) {
            System.err.println("Error: paramsList is empty or null");
            HashMap<String, Double> errorMetrics = new HashMap<>();
            errorMetrics.put("time", -1.0);
            errorMetrics.put("dbHits", -1.0);
            errorMetrics.put("memory", -1.0);
            return errorMetrics;
        }

        double totalTime = 0.0;
        double totalDbHits = 0.0;
        double totalMemory = 0.0;
        int validRuns = 0;

        // Run query for each parameter set
        for (HashMap<String, String> params : paramsList) {
            HashMap<String, Double> metrics = assess_single_query(query, params, session);

            // Only include results that didn't error (no -1 values)
            if (metrics.get("time") >= 0) {
                totalTime += metrics.get("time");
                totalDbHits += metrics.get("dbHits");
                totalMemory += metrics.get("memory");
                validRuns++;
            } else {
                System.err.println("Warning: Skipping failed query run");
            }
        }

        // Calculate averages
        HashMap<String, Double> averagedMetrics = new HashMap<>();
        if (validRuns > 0) {
            averagedMetrics.put("time", totalTime / validRuns);
            averagedMetrics.put("dbHits", totalDbHits / validRuns);
            averagedMetrics.put("memory", totalMemory / validRuns);
        } else {
            // All runs failed
            System.err.println("Error: All query runs failed");
            averagedMetrics.put("time", -1.0);
            averagedMetrics.put("dbHits", -1.0);
            averagedMetrics.put("memory", -1.0);
        }

        return averagedMetrics;
    }

    // queries is of form
    // KEY: "Vanilla"
    // VALUE: {query contents}
    /*
    private HashMap<String, HashMap<String, Double>> assess_query_dynamic_lockstep_base(
            HashMap<String, String> queries,
            int int_size,
            int run_count,
            Session session
    ){
        // Generate params
        ArrayList<HashMap<String, String>> mapping_list = generate_integer_range_mapping_list(run_count, int_size);

        HashMap<String, HashMap<String, Double>> results = new HashMap<>();

        for (Map.Entry<String, String> entry : mapping_list.entrySet())
            results.put(entry.getKey(), assess_query_averaged(entry.getValue(), mapping_list));

        return results;
    }

     */

    HashMap<String, HashMap<String, HashMap<String, Double>>> handle_one_size(
            int intSize,
            int runCount,
            int heat,
            String db_prefix,
            String qtype
    ){

        HashMap<String, HashMap<String, HashMap<String, Double>>> final_results = new HashMap<>();
        // Initialize database specific session
        try (Session session = this.driver.session(SessionConfig.forDatabase(db_prefix + "." + this.size_map.get(intSize)))) {
            // Iterate through all relevant queries
            // Key == Description
            // Value == {"Vanilla": "Vanilla query", "integer-range" : "integer-range query"}
            for (Map.Entry<String, HashMap<String, String>> entry : this.q_df.entrySet()){
                // System.out.println(entry.getKey());
                // System.out.println(entry.getValue());

                HashMap<String, String> innerMap = entry.getValue();
                HashMap<String, HashMap<String, Double>> descriptionResults = new HashMap<>();

                for (Map.Entry<String, String> innerEntry : innerMap.entrySet()){
                    // Warm up
                    ArrayList<HashMap<String, String>> heat_mapping_list = generate_integer_range_mapping_list(heat, intSize, qtype, session);
                    assess_query_averaged(innerEntry.getValue(), heat_mapping_list, session);

                    // Execute
                    ArrayList<HashMap<String, String>> run_mapping_list = generate_integer_range_mapping_list(runCount, intSize, qtype, session);
                    HashMap<String, Double> r = assess_query_averaged(
                            innerEntry.getValue(),
                            run_mapping_list,
                            session
                    );

                    // RECORD / STORE RESULTS
                    descriptionResults.put(innerEntry.getKey(), r);

                    // Clear cache
                    clearCaches(session);
                }

                final_results.put(entry.getKey(), descriptionResults);

            }
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
            e.printStackTrace();
        }

        return final_results;
    }

    private void clearCaches(Session session) {
        // Clear page cache
        session.run("CALL db.clearQueryCaches()").consume();

        // Add a small delay to ensure cache is cleared
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String getStringIdByName(int name, Session session) {
        String query = "MATCH (n {name: $name}) RETURN n.string_id AS string_id";

        Result result = session.run(query, Values.parameters("name", name));

        if (result.hasNext()) {
            Record record = result.next();
            return record.get("string_id").asString();
        }
        System.out.println("RETURNING NULL");
        return null;
    }

    public static void main(String[] args) {
        String uri = "bolt://localhost:7687";
        String username = "neo4j";
        String password = "giggleparabola"; // TrueBase
        // String password = "gigglediamond"; // UltraTall
        // String password = "gigglequadrilateral"; // UltraWide

        String csv_path = "/Users/danielarturi/Desktop/COMP 400/JavaWork/Solution3/src/main/java/QueryAssessment2/QueryDoc - Revised_NEO4J.csv";

        HashMap<Integer, String> size_map = new HashMap<>();
        size_map.put(10, "10");
        size_map.put(100, "100");
        size_map.put(1000, "1k");
        size_map.put(10000, "10k");
        size_map.put(100000, "100k");

        HashMap<String, HashMap<String, HashMap<String, HashMap<String, Double>>>> FINAL_RESULT = new HashMap<>();

        for (Map.Entry<Integer, String> entry : size_map.entrySet()){
            QueryAssessment instance_qa_obj = new QueryAssessment(
                    uri,
                    username,
                    password,
                    42L,
                    csv_path,
                    size_map
            );
            System.out.println("Starting size " + entry.getValue());
            HashMap<String, HashMap<String, HashMap<String, Double>>> x = instance_qa_obj.handle_one_size(
                    entry.getKey(),
                    10,
                    5,
                    "integer-range",
                    "int"
            );

            FINAL_RESULT.put(entry.getValue(), x);
        }

        try {
            ObjectMapper mapper = new ObjectMapper();

            // Write with pretty printing for readability
            mapper.writerWithDefaultPrettyPrinter()
                    .writeValue(new File("neo4j_results_TrueBase2.json"), FINAL_RESULT);

            System.out.println("Results successfully saved to query_results.json");

        } catch (IOException e) {
            System.err.println("Error writing JSON file: " + e.getMessage());
            e.printStackTrace();
        }
    }

}


