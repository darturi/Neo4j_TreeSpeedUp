package IndexCreation;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;
import java.util.Map;

public class IntegerIndexCreation1 implements AutoCloseable {

    private final Driver driver;
    private final String database;

    public IntegerIndexCreation1(String uri, String user, String password, String database) {
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
        String database = "integer.100k";

        /*
        try (IntegerIndexCreation1 app = new IntegerIndexCreation1(uri, user, password, database)) {
            app.complete_annotation(
                    "name",
                    1,
                    "TreeNode",
                    100,  // tree_id to assign to this subtree
                    "HAS_CHILD",
                    true
            );
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }

         */

        //annotate_n_forest(database, 10, uri, user, password, 5);
        //annotate_n_forest(database, 100, uri, user, password, 5);
        //annotate_n_forest(database, 1000, uri, user, password, 5);
        //annotate_n_forest(database, 10000, uri, user, password, 5);
        // annotate_n_forest(database, 100000, uri, user, password, 5);

        // annotate_n_forest_no_tree_id(database, 10, uri, user, password, 5);
        // annotate_n_forest(database, 100, uri, user, password, 5);
        // annotate_n_forest(database, 1000, uri, user, password, 5);
        // annotate_n_forest(database, 10000, uri, user, password, 5);
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
        try (IntegerIndexCreation1 app = new IntegerIndexCreation1(uri, user, password, db_name)) {
            Integer start_tree_id = 100;
            Integer start_node_name = 1;

            for (int i=0; i<n; i++){
                app.complete_annotation(
                        "name",
                        start_node_name,
                        "TreeNode",
                        start_tree_id,  // tree_id to assign to this subtree
                        "HAS_CHILD",
                        false
                );

                start_tree_id += 1;
                start_node_name += tree_size;
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void annotate_n_forest_no_tree_id(String db_name, Integer tree_size, String uri, String user, String password, Integer n){
        try (IntegerIndexCreation1 app = new IntegerIndexCreation1(uri, user, password, db_name)) {
            Integer start_node_name = 1;

            for (int i=0; i<n; i++){
                app.complete_annotation(
                        "name",
                        start_node_name,
                        "TreeNode",
                        "HAS_CHILD",
                        false,
                        start_node_name
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
     * @param treeId The tree_id to assign to all nodes in this subtree
     * @param relType The relationship type connecting parent to child
     * @param verbose Whether to print detailed progress information
     */
    public void complete_annotation(String idParamName,
                                    Integer idParamValue,
                                    String nodeMemberLabel,
                                    int treeId,
                                    String relType,
                                    boolean verbose) {
        // Step 1: Assign tree_id to all nodes in the subtree
        assignTreeId(idParamName, idParamValue, nodeMemberLabel, treeId, relType, verbose);

        // Step 2: Calculate interval widths bottom-up
        calculateIntervalWidths(idParamName, idParamValue, nodeMemberLabel, relType, verbose);

        // Step 3: Assign integer_id values top-down
        assignIntegerId(idParamName, idParamValue, nodeMemberLabel, relType, verbose);
    }

    public void complete_annotation(String idParamName,
                                    Integer idParamValue,
                                    String nodeMemberLabel,
                                    String relType,
                                    boolean verbose,
                                    Integer startIntegerId) {
        // Step 1: Assign tree_id to all nodes in the subtree
        // assignTreeId(idParamName, idParamValue, nodeMemberLabel, treeId, relType, verbose);

        // Step 2: Calculate interval widths bottom-up
        calculateIntervalWidths(idParamName, idParamValue, nodeMemberLabel, relType, verbose);

        // Step 3: Assign integer_id values top-down
        assignIntegerId(idParamName, idParamValue, nodeMemberLabel, relType, verbose, startIntegerId);
    }

    /**
     * Assign tree_id to all nodes in the subtree
     */
    private void assignTreeId(String idParamName,
                              Integer idParamValue,
                              String nodeMemberLabel,
                              int treeId,
                              String relType,
                              boolean verbose) {
        try (Session session = driver.session(getSessionConfig())) {
            session.executeWrite(tx -> {
                var query = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    MATCH p=(root)<-[rels*0..]-(node)
                    WHERE ALL(r IN rels WHERE type(r) = $relType)
                      AND $nodeMemberLabel IN labels(node)
                    WITH DISTINCT node
                    SET node.tree_id = $treeId
                    RETURN count(node) AS nodesUpdated
                """;

                Map<String, Object> params = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel,
                        "relType", relType,
                        "treeId", treeId
                );

                var result = tx.run(query, params);
                int nodesUpdated = result.single().get("nodesUpdated").asInt();

                if (verbose) {
                    System.out.println("Assigned tree_id = " + treeId + " to " + nodesUpdated + " nodes");
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error assigning tree_id: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Calculate interval widths for all nodes bottom-up
     * Leaves have interval_width = 1
     * Internal nodes have interval_width = sum(children.interval_width) + 1
     */
    private void calculateIntervalWidths(String idParamName,
                                         Integer idParamValue,
                                         String nodeMemberLabel,
                                         String relType,
                                         boolean verbose) {
        try (Session session = driver.session(getSessionConfig())) {
            session.executeWrite(tx -> {
                // Step 1: Set interval_width = 1 for all leaf nodes
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
                    SET leaf.interval_width = 1
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
                    System.out.println("Set interval_width = 1 for " + leafCount + " leaf nodes");
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
                        WHERE parent.interval_width IS NULL
                          AND EXISTS {
                            MATCH (parent)<-[r]-(child)
                            WHERE type(r) = $relType AND $nodeMemberLabel IN labels(child)
                          }
                        WITH parent,
                             [ (parent)<-[r]-(child)
                               WHERE type(r) = $relType AND $nodeMemberLabel IN labels(child) | child ] AS children
                        WHERE ALL(child IN children WHERE child.interval_width IS NOT NULL)
                        WITH parent,
                             reduce(sum = 0, child IN children |
                                sum + coalesce(child.interval_width, 0)) AS children_sum
                        SET parent.interval_width = children_sum + 1
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
                        System.out.println("Iteration " + iteration + ": Calculated interval_width for " + processedInIteration + " nodes");
                    }

                } while (processedInIteration > 0);

                if (verbose) {
                    System.out.println("Interval width calculation completed in " + iteration + " iterations");
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error calculating interval widths: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Assign integer_id values to all nodes top-down
     * Root gets integer_id = 1
     * Each child gets integer_id = parent.integer_id + sum(previous_siblings.interval_width) + 1
     */
    // TODO: RELTYPE NOT USED
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

                // Step 2: Process nodes level by level
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
                                offset + coalesce(children[prev_idx].interval_width, 0)
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

                    // Step 3: Verify results
                    var verifyQuery = """
                        MATCH (root)
                        WHERE $nodeMemberLabel IN labels(root)
                          AND root[$idParamName] = $idParamValue
                        MATCH p=(root)<-[*0..]-(node)
                        WHERE $nodeMemberLabel IN labels(node)
                        WITH DISTINCT node
                        RETURN node[$idParamName] AS nodeKey,
                               node.tree_id AS treeId,
                               node.depth AS depth,
                               node.integer_id AS integerId,
                               node.interval_width AS intervalWidth
                        ORDER BY depth, integerId
                    """;

                    Map<String, Object> verifyParams = Map.of(
                            "idParamName", idParamName,
                            "idParamValue", idParamValue,
                            "nodeMemberLabel", nodeMemberLabel
                    );

                    var verifyResult = tx.run(verifyQuery, verifyParams);

                    System.out.println("\n=== Integer Index assignments for subtree rooted at "
                            + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "] ===");
                    System.out.printf("%-20s %-10s %-8s %-12s %-15s%n", "Node Key", "Tree ID", "Depth", "Integer ID", "Interval Width");
                    System.out.println("-".repeat(75));

                    while (verifyResult.hasNext()) {
                        var record = verifyResult.next();
                        var nodeKey = record.get("nodeKey").asLong();
                        var treeId = record.get("treeId").isNull() ? null : record.get("treeId").asInt();
                        var depth = record.get("depth").asInt();
                        var integerId = record.get("integerId").isNull() ? null : record.get("integerId").asInt();
                        var intervalWidth = record.get("intervalWidth").isNull() ? null : record.get("intervalWidth").asInt();

                        System.out.printf("%-20s %-10s %-8d %-12s %-15s%n",
                                nodeKey,
                                treeId == null ? "null" : treeId.toString(),
                                depth,
                                integerId == null ? "null" : integerId.toString(),
                                intervalWidth == null ? "null" : intervalWidth.toString());
                    }
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
                                 boolean verbose,
                                 Integer startIntegerId) {
        try (Session session = driver.session(getSessionConfig())) {
            session.executeWrite(tx -> {
                // Step 1: Initialize root node with integer_id = startIntegerId and calculate depths
                var initQuery = """
                MATCH (root)
                WHERE $nodeMemberLabel IN labels(root)
                  AND root[$idParamName] = $idParamValue
                SET root.integer_id = $startIntegerId
                
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
                        "startIntegerId", startIntegerId
                );

                var initResult = tx.run(initQuery, initParams);
                var initRecord = initResult.single();
                int nodesProcessed = initRecord.get("nodesProcessed").asInt();
                int maxDepth = initRecord.get("maxDepth").asInt();

                if (verbose) {
                    System.out.println("Initialized " + nodesProcessed + " nodes with depths (max depth: " + maxDepth + ")");
                    System.out.println("Set root integer_id = " + startIntegerId);
                }

                // Step 2: Process nodes level by level
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
                            offset + coalesce(children[prev_idx].interval_width, 0)
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

                    // Step 3: Verify results
                    var verifyQuery = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    MATCH p=(root)<-[*0..]-(node)
                    WHERE $nodeMemberLabel IN labels(node)
                    WITH DISTINCT node
                    RETURN node[$idParamName] AS nodeKey,
                           node.tree_id AS treeId,
                           node.depth AS depth,
                           node.integer_id AS integerId,
                           node.interval_width AS intervalWidth
                    ORDER BY depth, integerId
                """;

                    Map<String, Object> verifyParams = Map.of(
                            "idParamName", idParamName,
                            "idParamValue", idParamValue,
                            "nodeMemberLabel", nodeMemberLabel
                    );

                    var verifyResult = tx.run(verifyQuery, verifyParams);

                    System.out.println("\n=== Integer Index assignments for subtree rooted at "
                            + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "] ===");
                    System.out.printf("%-20s %-10s %-8s %-12s %-15s%n", "Node Key", "Tree ID", "Depth", "Integer ID", "Interval Width");
                    System.out.println("-".repeat(75));

                    while (verifyResult.hasNext()) {
                        var record = verifyResult.next();
                        var nodeKey = record.get("nodeKey").asLong();
                        var treeId = record.get("treeId").isNull() ? null : record.get("treeId").asInt();
                        var depth = record.get("depth").asInt();
                        var integerId = record.get("integerId").isNull() ? null : record.get("integerId").asInt();
                        var intervalWidth = record.get("intervalWidth").isNull() ? null : record.get("intervalWidth").asInt();

                        System.out.printf("%-20s %-10s %-8d %-12s %-15s%n",
                                nodeKey,
                                treeId == null ? "null" : treeId.toString(),
                                depth,
                                integerId == null ? "null" : integerId.toString(),
                                intervalWidth == null ? "null" : intervalWidth.toString());
                    }
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error assigning integer IDs: " + e.getMessage());
            throw e;
        }
    }
}
