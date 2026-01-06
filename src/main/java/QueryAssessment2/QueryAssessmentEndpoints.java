package QueryAssessment2;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class QueryAssessmentEndpoints {
    private HashMap<Integer, String> size_map;

    private final String csv_path;
    private final String username;
    private final String password;
    private final String uri;
    private final String db_prefix;
    private final String qType;

    public QueryAssessmentEndpoints(String password, String qType){
        size_map = new HashMap<>();
        size_map.put(10, "10");
        size_map.put(100, "100");
        size_map.put(1000, "1k");
        size_map.put(10000, "10k");
        size_map.put(100000, "100k");

        this.password = password;
        this.username = "neo4j";
        this.uri = "bolt://localhost:7687";
        this.qType = qType;

        if (qType.equals("int")) {
            this.csv_path = "/Users/danielarturi/Desktop/COMP 400/JavaWork/Solution3/src/main/java/QueryAssessment2/QueryDoc - Revised_NEO4J.csv";
            this.db_prefix = "integer-range";
        }
        else if (qType.equals("string")) {
            this.csv_path = "/Users/danielarturi/Desktop/COMP 400/JavaWork/Solution3/src/main/java/QueryAssessment2/QueryDoc - Neo4j_String.csv";
            this.db_prefix = "string";
        } else {
            throw new IllegalArgumentException("Invalid qType");
        }
    }

    public void assess_full_suite(String save_file){
        HashMap<String, HashMap<String, HashMap<String, HashMap<String, Double>>>> FINAL_RESULT = new HashMap<>();

        for (Map.Entry<Integer, String> entry : size_map.entrySet()) {
            QueryAssessment qa_instance = new QueryAssessment(
                    this.uri,
                    this.username,
                    this.password,
                    42L,
                    this.csv_path,
                    this.size_map
            );

            System.out.println("Starting size " + entry.getValue());
            HashMap<String, HashMap<String, HashMap<String, Double>>> x = qa_instance.handle_one_size(
                    entry.getKey(),
                    10,
                    5,
                    this.db_prefix,
                    this.qType
            );

            FINAL_RESULT.put(entry.getValue(), x);
        }

        try {
            ObjectMapper mapper = new ObjectMapper();

            // Write with pretty printing for readability
            mapper.writerWithDefaultPrettyPrinter()
                    .writeValue(new File(save_file), FINAL_RESULT);

            System.out.println("Results successfully saved to save_file");

        } catch (IOException e) {
            System.err.println("Error writing JSON file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String truebase_password = "giggleparabola"; // TrueBase
        String ultratall_password = "gigglediamond"; // UltraTall
        String ultrawide_password = "gigglequadrilateral"; // UltraWide

        //QueryAssessmentEndpoints qae = new QueryAssessmentEndpoints(truebase_password, "string");
        //qae.assess_full_suite("string_assessment_truebase2.json");

        //QueryAssessmentEndpoints qae = new QueryAssessmentEndpoints(ultratall_password, "string");
        //qae.assess_full_suite("string_assessment_ultratall2.json");

        QueryAssessmentEndpoints qae = new QueryAssessmentEndpoints(ultrawide_password, "string");
        qae.assess_full_suite("string_assessment_ultrawide2.json");

        // qae = new QueryAssessmentEndpoints(truebase_password, "int");
        //qae.assess_full_suite("integer_range_assessment_truebase1.json");

        // qae = new QueryAssessmentEndpoints(ultratall_password, "int");
        // qae.assess_full_suite("integer_range_assessment_ultratall1.json");

        // qae = new QueryAssessmentEndpoints(ultrawide_password, "int");
        // qae.assess_full_suite("integer_range_assessment_ultrawide1.json");
    }

}
