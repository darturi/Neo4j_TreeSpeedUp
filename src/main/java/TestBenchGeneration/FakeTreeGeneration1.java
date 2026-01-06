package TestBenchGeneration;

import org.neo4j.driver.*;
import java.util.*;

public class FakeTreeGeneration1 {

    private final Driver driver;
    private final String database;

    public FakeTreeGeneration1(String uri, String user, String password, String database) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        this.database = database;
    }

    public void close() {
        driver.close();
    }

    public void createTree(int numberOfNodes, int childrenLowerBound,
                           int childrenUpperBound, int maxDepth, long seed) {

        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            // Clear existing tree if needed
            session.run("MATCH (n:TreeNode) DETACH DELETE n");

            int runningBaseId = 1;
            Random random = new Random(seed);

            // Create root node
            session.run(
                    "CREATE (n:TreeNode {name: $name, depth: $depth})",
                    Map.of("name", runningBaseId, "depth", 0)
            );

            numberOfNodes--;
            runningBaseId++;

            // Queue for BFS traversal
            Queue<NodeInfo> childless = new LinkedList<>();
            childless.add(new NodeInfo(1, 0));

            while (!childless.isEmpty() && numberOfNodes > 0) {
                NodeInfo currentNode = childless.poll();

                int newChildCount = random.nextInt(childrenUpperBound - childrenLowerBound + 1)
                        + childrenLowerBound;

                for (int i = 0; i < newChildCount && numberOfNodes > 0; i++) {
                    int childId = runningBaseId;
                    int childDepth = currentNode.depth + 1;

                    // Create child node and relationship to parent
                    session.run(
                            "MATCH (parent:TreeNode {name: $parentName}) " +
                                    "CREATE (child:TreeNode {name: $childName, depth: $depth}) " +
                                    "CREATE (child)-[:HAS_CHILD]->(parent)",
                            Map.of(
                                    "parentName", currentNode.name,
                                    "childName", childId,
                                    "depth", childDepth
                            )
                    );

                    runningBaseId++;
                    numberOfNodes--;

                    if (childDepth < maxDepth) {
                        childless.add(new NodeInfo(childId, childDepth));
                    }
                }
            }

            // Annotate heights after tree construction
            annotateHeights();
        }
    }


    public void createTree(int numberOfNodes, int childrenLowerBound,
                           int childrenUpperBound, int maxDepth, Integer start_name, long seed) {

        try (Session session = driver.session(SessionConfig.forDatabase(database))) {

            int runningBaseId = start_name;
            Random random = new Random(seed);

            // Create root node
            session.run(
                    "CREATE (n:TreeNode {name: $name, depth: $depth})",
                    Map.of("name", runningBaseId, "depth", 0)
            );

            numberOfNodes--;
            runningBaseId++;

            // Queue for BFS traversal
            Queue<NodeInfo> childless = new LinkedList<>();
            childless.add(new NodeInfo(runningBaseId - 1, 0));

            while (!childless.isEmpty() && numberOfNodes > 0) {
                NodeInfo currentNode = childless.poll();

                int newChildCount = random.nextInt(childrenUpperBound - childrenLowerBound + 1)
                        + childrenLowerBound;

                for (int i = 0; i < newChildCount && numberOfNodes > 0; i++) {
                    int childId = runningBaseId;
                    int childDepth = currentNode.depth + 1;

                    // Create child node and relationship to parent
                    session.run(
                            "MATCH (parent:TreeNode {name: $parentName}) " +
                                    "CREATE (child:TreeNode {name: $childName, depth: $depth}) " +
                                    "CREATE (child)-[:HAS_CHILD]->(parent)",
                            Map.of(
                                    "parentName", currentNode.name,
                                    "childName", childId,
                                    "depth", childDepth
                            )
                    );

                    runningBaseId++;
                    numberOfNodes--;

                    if (childDepth < maxDepth) {
                        childless.add(new NodeInfo(childId, childDepth));
                    }
                }
            }

            // Annotate heights after tree construction
            annotateHeights();
        }
    }

    // Overloaded method for backward compatibility (uses random seed)
    public void createTree(int numberOfNodes, int childrenLowerBound,
                           int childrenUpperBound, int maxDepth) {
        createTree(numberOfNodes, childrenLowerBound, childrenUpperBound, maxDepth,
                System.currentTimeMillis());
    }

    public void annotateHeights() {
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            // Find all leaf nodes (nodes with no incoming relationships) and set their height to 0
            // Since arrows are reversed, leaves have no incoming HAS_CHILD relationships
            session.run(
                    "MATCH (n:TreeNode) " +
                            "WHERE NOT (n)<-[:HAS_CHILD]-() " +
                            "SET n.height = 0"
            );

            // Iteratively compute heights from leaves up to root
            // Since arrows are reversed, we traverse outgoing relationships from children
            boolean updated = true;
            while (updated) {
                Result result = session.run(
                        "MATCH (child:TreeNode)-[:HAS_CHILD]->(parent:TreeNode) " +
                                "WHERE child.height IS NOT NULL " +
                                "WITH parent, max(child.height) AS maxChildHeight " +
                                "WHERE parent.height IS NULL OR parent.height < maxChildHeight + 1 " +
                                "SET parent.height = maxChildHeight + 1 " +
                                "RETURN count(parent) AS updatedCount"
                );

                long updatedCount = result.single().get("updatedCount").asLong();
                updated = updatedCount > 0;
            }
        }
    }

    // Helper class to store node information during traversal
    private static class NodeInfo {
        int name;
        int depth;

        NodeInfo(int name, int depth) {
            this.name = name;
            this.depth = depth;
        }
    }

    public static void main(String[] args) {
        // Example usage
        /*
        FakeTreeGeneration1 generator = new FakeTreeGeneration1(
                "bolt://localhost:7687",
                "neo4j",
                "gigglerhombus",
                "integer.10"  // database name
        );

        try {
            // Using seed 42 for reproducible results
            generator.createTree(10, 1, 3, 20, 42L);
            // generator.createTree(100, 1, 4, 20, 42L);
            // generator.createTree(1000, 2, 5, 20, 42L);
            // generator.createTree(10000, 3, 11, 20, 42L);
            // generator.createTree(100000, 3, 20, 20, 42L);
            System.out.println("Tree created successfully!");
        } finally {
            generator.close();
        }

         */

        // populate_ultra_tall(42L);
        // populate_ultra_wide(42L);
        // populate_n_trees(1, 42L, "giggleswirl");
        populate_n_trees(1, 42L, "giggleparabola");
    }

    // Populating the UltraTall Instance

    private static Map<String, Map<String, Integer>> set_base_bench_creation_param(){
        Map<String, Map<String, Integer>> param_dict = new HashMap<>();
        param_dict.put("10", new HashMap<>());
        param_dict.put("100", new HashMap<>());
        param_dict.put("1k", new HashMap<>());
        param_dict.put("10k", new HashMap<>());
        param_dict.put("100k", new HashMap<>());

        param_dict.get("10").put("size", 10);
        param_dict.get("100").put("size", 100);
        param_dict.get("1k").put("size", 1000);
        param_dict.get("10k").put("size", 10000);
        param_dict.get("100k").put("size", 100000);

        return param_dict;
    }
    public static void populate_ultra_tall(Long seed){
        Map<String, Map<String, Integer>> param_dict = set_base_bench_creation_param();

        // Set 10 params
        param_dict.get("10").put("lb", 1);
        param_dict.get("10").put("ub", 3);
        param_dict.get("10").put("md", 10);

        // Set 100 params
        param_dict.get("100").put("lb", 1);
        param_dict.get("100").put("ub", 3);
        param_dict.get("100").put("md", 75);

        // Set 1k params
        param_dict.get("1k").put("lb", 1);
        param_dict.get("1k").put("ub", 3);
        param_dict.get("1k").put("md", 600);

        // Set 10k params
        param_dict.get("10k").put("lb", 1);
        param_dict.get("10k").put("ub", 3);
        param_dict.get("10k").put("md", 5000);

        // Set 100k params
        param_dict.get("100k").put("lb", 1);
        param_dict.get("100k").put("ub", 3);
        param_dict.get("100k").put("md", 40000);

        populate_bench(param_dict, "gigglediamond", seed);
    }

    public static void populate_n_trees(Integer n, Long seed, String password){
        // List<String> index_classes = Arrays.asList("float", "string", "integer");
        //List<String> index_classes = Arrays.asList("integer-no-tree-id");
        //List<String> index_classes = Arrays.asList("integer-range");
        List<String> index_classes = Arrays.asList("float");
        List<String> tree_sizes = Arrays.asList("10");
        // List<String> tree_sizes = Arrays.asList("10" , "100", "1k", "10k", "100k");
        //List<String> tree_sizes = Arrays.asList("100");

        Map<String, Map<String, Integer>> param_dict = set_base_bench_creation_param();

        // Set up 10
        param_dict.get("10").put("lb", 1);
        param_dict.get("10").put("ub", 3);
        param_dict.get("10").put("md", 20);

        // Set up 100
        param_dict.get("100").put("lb", 1);
        param_dict.get("100").put("ub", 4);
        param_dict.get("100").put("md", 20);

        // Set up 1k
        param_dict.get("1k").put("lb", 2);
        param_dict.get("1k").put("ub", 5);
        param_dict.get("1k").put("md", 20);

        // Set up 10k
        param_dict.get("10k").put("lb", 3);
        param_dict.get("10k").put("ub", 4);
        param_dict.get("10k").put("md", 11);

        // Set up 100k
        param_dict.get("100k").put("lb", 3);
        param_dict.get("100k").put("ub", 20);
        param_dict.get("100k").put("md", 20);


        for (String ic : index_classes){
            for (String ts : tree_sizes){
                String db_name = String.format("%s.%s", ic, ts);

                create_n_trees(password, db_name, n, param_dict.get(ts), seed);

                System.out.println(String.format("done with %s", db_name));
            }
        }
    }

    private static void create_n_trees(String password, String db, Integer n, Map<String, Integer> params, Long seed){
        Integer running_base = 1;

        FakeTreeGeneration1 generator = new FakeTreeGeneration1(
                "bolt://localhost:7687",
                "neo4j",
                password,
                db  // database name
        );

        try {
            for (int i=0; i<n; i++){
                generator.createTree(
                        params.get("size"),
                        params.get("lb"),
                        params.get("ub"),
                        params.get("md"),
                        running_base,
                        seed
                );
                running_base += params.get("size");
            }
            System.out.println("Forest created successfully!");
        } finally {
            generator.close();
        }
    }

    public static void populate_ultra_wide(Long seed){
        Map<String, Map<String, Integer>> param_dict = set_base_bench_creation_param();

        // Set 10 params
        param_dict.get("10").put("lb", 2);
        param_dict.get("10").put("ub", 4);
        param_dict.get("10").put("md", 3);

        // Set 100 params
        param_dict.get("100").put("lb", 4);
        param_dict.get("100").put("ub", 6);
        param_dict.get("100").put("md", 4);

        // Set 1k params
        param_dict.get("1k").put("lb", 7);
        param_dict.get("1k").put("ub", 9);
        param_dict.get("1k").put("md", 5);

        // Set 10k params
        param_dict.get("10k").put("lb", 9);
        param_dict.get("10k").put("ub", 11);
        param_dict.get("10k").put("md", 6);

        // Set 100k params
        param_dict.get("100k").put("lb", 11);
        param_dict.get("100k").put("ub", 13);
        param_dict.get("100k").put("md", 7);

        populate_bench(param_dict, "gigglequadrilateral", seed);
    }

    public static void populate_bench(Map<String, Map<String, Integer>> param_dict, String password, Long seed){
        String uri = "bolt://localhost:7687";
        String user = "neo4j";

        // List<String> index_classes = Arrays.asList("float", "string", "integer");
        // List<String> index_classes = Arrays.asList("integer-range");
        List<String> index_classes = Arrays.asList("float");
        List<String> tree_sizes = Arrays.asList("10");//, "100", "1k", "10k", "100k");

        for (String ic : index_classes){
            for (String ts : tree_sizes){
                FakeTreeGeneration1 generator = new FakeTreeGeneration1(
                        uri,
                        user,
                        password,
                        String.format("%s.%s", ic, ts)  // database name
                );

                Map<String, Integer> curr_params = param_dict.get(ts);

                try {
                    generator.createTree(
                            curr_params.get("size"),
                            curr_params.get("lb"),
                            curr_params.get("ub"),
                            curr_params.get("md"),
                            seed
                    );
                    System.out.println("Tree created successfully!");
                } finally {
                    generator.close();
                }
            }
        }
    }
}