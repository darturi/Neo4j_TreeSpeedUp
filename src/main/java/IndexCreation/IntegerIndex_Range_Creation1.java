package IndexCreation;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;

import java.util.Map;

public class IntegerIndex_Range_Creation1 implements AutoCloseable {

    private final Driver driver;
    private final String database;

    public IntegerIndex_Range_Creation1(String uri, String user, String password, String database) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        this.database = database;
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public static void main(String[] args) {
        // Connection details - adjust these for your Neo4j instance
        String uri = "neo4j://127.0.0.1:7687";  // Default Neo4j bolt port
        String user = "neo4j";
        String password = "giggleparabola";  // Change this to your actual password
        String database = "integer-range.100k";

        /*
        try (IntegerIndex_Range_Creation1 app = new IntegerIndex_Range_Creation1(uri, user, password, database)) {
            app.complete_annotation(
                    "name",
                    1,
                    "TreeNode",
                    "HAS_CHILD",
                    true
            );
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        */

        // annotate_n_forest(database, 10, uri, user, password, 1);
        // annotate_n_forest(database, 100, uri, user, password, 1);
        //annotate_n_forest(database, 1000, uri, user, password, 1);
        // annotate_n_forest(database, 10000, uri, user, password, 1);
        annotate_n_forest(database, 100000, uri, user, password, 1);
    }

    public Object getNodeAttribute(String nodeLabel,
                                   String searchAttrName,
                                   Integer searchAttrValue,
                                   String returnAttrName) {
        try (Session session = driver.session(getSessionConfig())) {
            return session.executeRead(tx -> {
                var query = """
                MATCH (n)
                WHERE $nodeLabel IN labels(n)
                  AND n[$searchAttrName] = $searchAttrValue
                RETURN n[$returnAttrName] AS attributeValue
                LIMIT 1
            """;

                Map<String, Object> params = Map.of(
                        "nodeLabel", nodeLabel,
                        "searchAttrName", searchAttrName,
                        "searchAttrValue", searchAttrValue,
                        "returnAttrName", returnAttrName
                );

                var result = tx.run(query, params);

                if (result.hasNext()) {
                    var record = result.next();
                    if (!record.get("attributeValue").isNull()) {
                        // Return the appropriate type based on the value
                        var value = record.get("attributeValue");
                        if (value.hasType(org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM.INTEGER())) {
                            return value.asLong();
                        } else if (value.hasType(org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM.FLOAT())) {
                            return value.asDouble();
                        } else if (value.hasType(org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM.STRING())) {
                            return value.asString();
                        } else if (value.hasType(org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM.BOOLEAN())) {
                            return value.asBoolean();
                        } else {
                            return value.asObject();
                        }
                    }
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error retrieving attribute '" + returnAttrName
                    + "' from " + nodeLabel + "[" + searchAttrName + "=" + searchAttrValue + "]: "
                    + e.getMessage());
            throw e;
        }
    }

    public static void annotate_n_forest(String db_name, Integer tree_size, String uri, String user, String password, Integer n){
        try (IntegerIndex_Range_Creation1 app = new IntegerIndex_Range_Creation1(uri, user, password, db_name)) {
            Integer start_node_name = 1;

            for (int i=0; i<n; i++){
                app.complete_annotation(
                        "name",
                        start_node_name,
                        "TreeNode",
                        "HAS_CHILD",
                        start_node_name,
                        false
                );

                start_node_name += tree_size;
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private SessionConfig getSessionConfig() {
        return SessionConfig.builder()
                .withDatabase(database)
                .build();
    }

    /**
     * Complete annotation process for a tree structure
     * @param idParamName The property name used to identify the root node
     * @param idParamValue The property value used to identify the root node
     * @param nodeMemberLabel The label for nodes in the tree
     * @param relType The relationship type connecting parent to child
     * @param verbose Whether to print detailed progress information
     */
    public void complete_annotation(String idParamName,
                                    Integer idParamValue,
                                    String nodeMemberLabel,
                                    String relType,
                                    boolean verbose) {

        // Step 1: Calculate subtree sizes for all nodes (bottom-up)
        calculateSubtreeSizes(idParamName, idParamValue, nodeMemberLabel, relType, verbose);

        // Step 2: Assign integer_id values using pre-order traversal (top-down)
        assignIntegerId(idParamName, idParamValue, nodeMemberLabel, relType, verbose);

        // Step 3: Calculate upper bounds based on integer_id + subtree_size - 1
        calculateUpperBounds(idParamName, idParamValue, nodeMemberLabel, verbose);
    }

    public void complete_annotation(String idParamName,
                                    Integer idParamValue,
                                    String nodeMemberLabel,
                                    String relType,
                                    int startingIntegerId,
                                    boolean verbose) {

        // Step 1: Calculate subtree sizes for all nodes (bottom-up)
        calculateSubtreeSizes(idParamName, idParamValue, nodeMemberLabel, relType, verbose);

        // Step 2: Assign integer_id values using pre-order traversal (top-down)
        assignIntegerId(idParamName, idParamValue, nodeMemberLabel, relType, startingIntegerId, verbose);

        // Step 3: Calculate upper bounds based on integer_id + subtree_size - 1
        calculateUpperBounds(idParamName, idParamValue, nodeMemberLabel, verbose);
    }

    /**
     * Calculate subtree sizes for all nodes bottom-up
     * Leaves have subtree_size = 1
     * Internal nodes have subtree_size = sum(children.subtree_size) + 1
     */
    private void calculateSubtreeSizes(String idParamName,
                                       Integer idParamValue,
                                       String nodeMemberLabel,
                                       String relType,
                                       boolean verbose) {
        try (Session session = driver.session(getSessionConfig())) {
            session.executeWrite(tx -> {
                // Step 1: Set subtree_size = 1 for all leaf nodes
                var leafQuery = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    MATCH p=(root)<-[rels*0..]-(leaf)
                    WHERE ALL(r IN rels WHERE type(r) = $relType)
                      AND $nodeMemberLabel IN labels(leaf)
                      AND NOT EXISTS {
                        MATCH (leaf)<-[r2]-(c2)
                        WHERE type(r2) = $relType AND $nodeMemberLabel IN labels(c2)
                      }
                    WITH DISTINCT leaf
                    SET leaf.subtree_size = 1
                    RETURN count(leaf) AS leafCount
                """;

                Map<String, Object> leafParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel,
                        "relType", relType
                );

                var leafResult = tx.run(leafQuery, leafParams);
                int leafCount = leafResult.single().get("leafCount").asInt();

                if (verbose) {
                    System.out.println("Set subtree_size = 1 for " + leafCount + " leaf nodes");
                }

                // Step 2: Bottom-up processing for internal nodes
                int iteration = 0;
                int processedInIteration;

                do {
                    iteration++;
                    var processQuery = """
                        MATCH (root)
                        WHERE $nodeMemberLabel IN labels(root)
                          AND root[$idParamName] = $idParamValue
                        MATCH p=(root)<-[rels*0..]-(parent)
                        WHERE ALL(r IN rels WHERE type(r) = $relType)
                          AND $nodeMemberLabel IN labels(parent)
                        WITH DISTINCT parent
                        WHERE parent.subtree_size IS NULL
                          AND EXISTS {
                            MATCH (parent)<-[r]-(child)
                            WHERE type(r) = $relType AND $nodeMemberLabel IN labels(child)
                          }
                        WITH parent,
                             [ (parent)<-[r]-(child)
                               WHERE type(r) = $relType AND $nodeMemberLabel IN labels(child) | child ] AS children
                        WHERE ALL(child IN children WHERE child.subtree_size IS NOT NULL)
                        WITH parent,
                             reduce(sum = 0, child IN children |
                                sum + coalesce(child.subtree_size, 0)) AS children_sum
                        SET parent.subtree_size = children_sum + 1
                        RETURN count(parent) AS processed
                    """;

                    Map<String, Object> processParams = Map.of(
                            "idParamName", idParamName,
                            "idParamValue", idParamValue,
                            "nodeMemberLabel", nodeMemberLabel,
                            "relType", relType
                    );

                    var processResult = tx.run(processQuery, processParams);
                    processedInIteration = processResult.single().get("processed").asInt();

                    if (verbose && processedInIteration > 0) {
                        System.out.println("Iteration " + iteration + ": Calculated subtree_size for " + processedInIteration + " nodes");
                    }

                } while (processedInIteration > 0);

                if (verbose) {
                    System.out.println("Subtree size calculation completed in " + iteration + " iterations");
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error calculating subtree sizes: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Assign integer_id values to all nodes top-down using pre-order traversal
     * Root gets integer_id = 1
     * Each child gets integer_id = parent.integer_id + sum(previous_siblings.subtree_size) + 1
     */
    private void assignIntegerId(String idParamName,
                                 Integer idParamValue,
                                 String nodeMemberLabel,
                                 String relType,
                                 boolean verbose) {
        try (Session session = driver.session(getSessionConfig())) {
            session.executeWrite(tx -> {
                // Step 1: Initialize root node with integer_id = 1 and calculate depths
                var initQuery = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    SET root.integer_id = 1
                    
                    WITH root
                    MATCH p=(root)<-[*0..]-(node)
                    WHERE $nodeMemberLabel IN labels(node)
                    WITH root, node, length(p) AS node_depth
                    SET node.depth = node_depth
                    
                    RETURN count(node) AS nodesProcessed, max(node_depth) AS maxDepth
                """;

                Map<String, Object> initParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel
                );

                var initResult = tx.run(initQuery, initParams);
                var initRecord = initResult.single();
                int nodesProcessed = initRecord.get("nodesProcessed").asInt();
                int maxDepth = initRecord.get("maxDepth").asInt();

                if (verbose) {
                    System.out.println("Initialized " + nodesProcessed + " nodes with depths (max depth: " + maxDepth + ")");
                    System.out.println("Set root integer_id = 1");
                }

                // Step 2: Process nodes level by level (top-down, pre-order)
                for (int currentLevel = 1; currentLevel <= maxDepth; currentLevel++) {
                    var levelProcessQuery = """
                        MATCH (root)
                        WHERE $nodeMemberLabel IN labels(root)
                          AND root[$idParamName] = $idParamValue
                        
                        // Find parents at the previous level with assigned integer_id
                        MATCH p=(root)<-[*0..]-(parent)
                        WHERE $nodeMemberLabel IN labels(parent)
                          AND parent.depth = $parentDepth
                          AND parent.integer_id IS NOT NULL
                        
                        WITH DISTINCT parent
                        WHERE EXISTS {
                          MATCH (parent)<-[]-(child)
                          WHERE $nodeMemberLabel IN labels(child)
                        }
                        
                        WITH parent,
                             [ (parent)<-[]-(child)
                               WHERE $nodeMemberLabel IN labels(child) | child ] AS children
                        
                        UNWIND range(0, size(children) - 1) AS childIndex
                        WITH parent, children, childIndex,
                             children[childIndex] AS currentChild,
                             reduce(offset = 0,
                                prev_idx IN range(0, childIndex - 1) |
                                offset + coalesce(children[prev_idx].subtree_size, 0)
                             ) AS accumulated_offset
                        
                        SET currentChild.integer_id = parent.integer_id + accumulated_offset + 1
                        
                        RETURN count(currentChild) AS childrenProcessed
                    """;

                    Map<String, Object> levelParams = Map.of(
                            "idParamName", idParamName,
                            "idParamValue", idParamValue,
                            "nodeMemberLabel", nodeMemberLabel,
                            "parentDepth", currentLevel - 1
                    );

                    var levelResult = tx.run(levelProcessQuery, levelParams);

                    int childrenProcessed = 0;
                    if (levelResult.hasNext()) {
                        childrenProcessed = levelResult.stream()
                                .mapToInt(record -> record.get("childrenProcessed").asInt())
                                .sum();
                    }

                    if (verbose && childrenProcessed > 0) {
                        System.out.println("Level " + currentLevel + ": Assigned integer_id to " + childrenProcessed + " nodes");
                    }
                }

                if (verbose) {
                    System.out.println("Integer ID assignment completed for " + maxDepth + " levels");
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error assigning integer IDs: " + e.getMessage());
            throw e;
        }
    }


    private void assignIntegerId(String idParamName,
                                 Integer idParamValue,
                                 String nodeMemberLabel,
                                 String relType,
                                 int startingIntegerId,
                                 boolean verbose) {
        try (Session session = driver.session(getSessionConfig())) {
            session.executeWrite(tx -> {
                // Step 1: Initialize root node with the specified integer_id and calculate depths
                var initQuery = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    SET root.integer_id = $startingIntegerId
                    
                    WITH root
                    MATCH p=(root)<-[*0..]-(node)
                    WHERE $nodeMemberLabel IN labels(node)
                    WITH root, node, length(p) AS node_depth
                    SET node.depth = node_depth
                    
                    RETURN count(node) AS nodesProcessed, max(node_depth) AS maxDepth
                """;

                Map<String, Object> initParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel,
                        "startingIntegerId", startingIntegerId
                );

                var initResult = tx.run(initQuery, initParams);
                var initRecord = initResult.single();
                int nodesProcessed = initRecord.get("nodesProcessed").asInt();
                int maxDepth = initRecord.get("maxDepth").asInt();

                if (verbose) {
                    System.out.println("Initialized " + nodesProcessed + " nodes with depths (max depth: " + maxDepth + ")");
                    System.out.println("Set root integer_id = " + startingIntegerId);
                }

                // Step 2: Process nodes level by level (top-down, pre-order)
                for (int currentLevel = 1; currentLevel <= maxDepth; currentLevel++) {
                    var levelProcessQuery = """
                        MATCH (root)
                        WHERE $nodeMemberLabel IN labels(root)
                          AND root[$idParamName] = $idParamValue
                        
                        // Find parents at the previous level with assigned integer_id
                        MATCH p=(root)<-[*0..]-(parent)
                        WHERE $nodeMemberLabel IN labels(parent)
                          AND parent.depth = $parentDepth
                          AND parent.integer_id IS NOT NULL
                        
                        WITH DISTINCT parent
                        WHERE EXISTS {
                          MATCH (parent)<-[]-(child)
                          WHERE $nodeMemberLabel IN labels(child)
                        }
                        
                        WITH parent,
                             [ (parent)<-[]-(child)
                               WHERE $nodeMemberLabel IN labels(child) | child ] AS children
                        
                        UNWIND range(0, size(children) - 1) AS childIndex
                        WITH parent, children, childIndex,
                             children[childIndex] AS currentChild,
                             reduce(offset = 0,
                                prev_idx IN range(0, childIndex - 1) |
                                offset + coalesce(children[prev_idx].subtree_size, 0)
                             ) AS accumulated_offset
                        
                        SET currentChild.integer_id = parent.integer_id + accumulated_offset + 1
                        
                        RETURN count(currentChild) AS childrenProcessed
                    """;

                    Map<String, Object> levelParams = Map.of(
                            "idParamName", idParamName,
                            "idParamValue", idParamValue,
                            "nodeMemberLabel", nodeMemberLabel,
                            "parentDepth", currentLevel - 1
                    );

                    var levelResult = tx.run(levelProcessQuery, levelParams);

                    int childrenProcessed = 0;
                    if (levelResult.hasNext()) {
                        childrenProcessed = levelResult.stream()
                                .mapToInt(record -> record.get("childrenProcessed").asInt())
                                .sum();
                    }

                    if (verbose && childrenProcessed > 0) {
                        System.out.println("Level " + currentLevel + ": Assigned integer_id to " + childrenProcessed + " nodes");
                    }
                }

                if (verbose) {
                    System.out.println("Integer ID assignment completed for " + maxDepth + " levels");
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error assigning integer IDs: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Calculate upper bounds for all nodes
     * For each node: upper_bound = integer_id + subtree_size - 1
     */
    private void calculateUpperBounds(String idParamName,
                                      Integer idParamValue,
                                      String nodeMemberLabel,
                                      boolean verbose) {
        try (Session session = driver.session(getSessionConfig())) {
            session.executeWrite(tx -> {
                var query = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    MATCH p=(root)<-[*0..]-(node)
                    WHERE $nodeMemberLabel IN labels(node)
                    WITH DISTINCT node
                    SET node.upper_bound = node.integer_id + node.subtree_size - 1
                    RETURN count(node) AS nodesUpdated
                """;

                Map<String, Object> params = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel
                );

                var result = tx.run(query, params);
                int nodesUpdated = result.single().get("nodesUpdated").asInt();

                if (verbose) {
                    System.out.println("Calculated upper_bound for " + nodesUpdated + " nodes");
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error calculating upper bounds: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Verify and display the range assignments
     */
    public void verifyRangeAssignments(String idParamName,
                                       Integer idParamValue,
                                       String nodeMemberLabel) {
        try (Session session = driver.session(getSessionConfig())) {
            session.executeRead(tx -> {
                var verifyQuery = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    MATCH p=(root)<-[*0..]-(node)
                    WHERE $nodeMemberLabel IN labels(node)
                    WITH DISTINCT node
                    RETURN node[$idParamName] AS nodeKey,
                           node.depth AS depth,
                           node.integer_id AS lowBound,
                           node.upper_bound AS upBound,
                           node.subtree_size AS subtreeSize
                    ORDER BY depth, lowBound
                """;

                Map<String, Object> verifyParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel
                );

                var verifyResult = tx.run(verifyQuery, verifyParams);

                System.out.println("\n=== Integer Range Index assignments for subtree rooted at "
                        + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "] ===");
                System.out.printf("%-20s %-8s %-15s %-15s %-15s%n", "Node Key", "Depth", "Lower Bound", "Upper Bound", "Subtree Size");
                System.out.println("-".repeat(80));

                while (verifyResult.hasNext()) {
                    var record = verifyResult.next();
                    var nodeKey = record.get("nodeKey").asLong();
                    var depth = record.get("depth").asInt();
                    var lowBound = record.get("lowBound").isNull() ? null : record.get("lowBound").asInt();
                    var upBound = record.get("upBound").isNull() ? null : record.get("upBound").asInt();
                    var subtreeSize = record.get("subtreeSize").isNull() ? null : record.get("subtreeSize").asInt();

                    System.out.printf("%-20s %-8d %-15s %-15s %-15s%n",
                            nodeKey,
                            depth,
                            lowBound == null ? "null" : lowBound.toString(),
                            upBound == null ? "null" : upBound.toString(),
                            subtreeSize == null ? "null" : subtreeSize.toString());
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error verifying range assignments: " + e.getMessage());
            throw e;
        }
    }
}