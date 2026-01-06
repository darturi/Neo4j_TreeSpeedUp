package IndexCreation;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FloatIndexCreation1 implements AutoCloseable {

    private final Driver driver;
    private final String database;

    public FloatIndexCreation1(String uri, String user, String password, String database) {
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
        String database = "float.100k";

        /*

        try (FloatIndexCreation1 app = new FloatIndexCreation1(uri, user, password, database)) {
            app.complete_annotation(
                    "name",
                    1,
                    "TreeNode",
                    1.0,
                    "float_id",
                    "HAS_CHILD",
                    false
            );
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }

         */
        annotate_n_forest(
                database,
                100000,
                uri,
                user,
                password,
                1
        );
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
        try (FloatIndexCreation1 app = new FloatIndexCreation1(uri, user, password, db_name)) {
            double start_node_id = 1.0;
            Integer start_node_name = 1;

            for (int i=0; i<n; i++){
                app.complete_annotation(
                        "name",
                        start_node_name,
                        "TreeNode",
                        start_node_id,
                        "float_id",
                        "HAS_CHILD",
                        false
                );

                start_node_id += (double) app.getNodeAttribute(
                        "TreeNode",
                        "name",
                        start_node_name,
                        "interval_width"
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

    // RELATIONSHIP AGNOSTIC
    public void complete_annotation(String idParamName,
                                    Integer idParamValue,
                                    String nodeMemberLabel,
                                    double rootId,
                                    String newIdName,
                                    String relType,
                                    boolean verbose
    ){
        handleSpaceRequests(
                idParamName,
                idParamValue,
                nodeMemberLabel,
                relType
        );
        handleIdAssignment(
                idParamName,
                idParamValue,
                nodeMemberLabel,
                rootId,
                newIdName,
                verbose
        );

    }


    // SINGLE RELATIONSHIP TYPE
    public void handleSpaceRequests(String idParamName,
                                    Integer idParamValue,
                                    String nodeMemberLabel,
                                    String relType) {
        try (Session session = driver.session(getSessionConfig())) {
            session.executeWrite(tx -> {
                // Step 1: Set interval_width = 1 for all leaf nodes in the subtree
                // With reversed arrows (child->parent), traverse DOWN by following INCOMING arrows
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
                System.out.println("Set interval_width = 1 for " + leafCount + " leaf nodes");

                // Step 2: Bottom-up processing
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
                             reduce(space_request = 0.0, child IN children |
                                space_request + coalesce(child.interval_width, 0.0)) AS space_request
                        SET parent.interval_width = toFloat(floor(space_request / 10.0) + 1.0)
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

                    if (processedInIteration > 0) {
                        System.out.println("Iteration " + iteration + ": Processed " + processedInIteration + " nodes");
                    }

                } while (processedInIteration > 0);

                System.out.println("Bottom-up processing completed in " + iteration + " iterations");

                return null;
            });
        }
    }

    public void handleIdAssignment(String idParamName,
                                   Integer idParamValue,
                                   String nodeMemberLabel,
                                   double rootId,
                                   String idName,
                                   boolean verbose) {
        try (Session session = driver.session(getSessionConfig())) {
            session.executeWrite(tx -> {
                // Step 1: Initialize root node and calculate depths
                // With reversed arrows (child->parent), traverse DOWN from root following INCOMING arrows
                var initQuery = """
            MATCH (root)
            WHERE $nodeMemberLabel IN labels(root)
              AND root[$idParamName] = $idParamValue
            SET root[$idName] = toFloat($rootId)

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
                        "rootId", rootId,
                        "idName", idName
                );

                var initResult = tx.run(initQuery, initParams);
                var initRecord = initResult.single();
                int nodesProcessed = initRecord.get("nodesProcessed").asInt();
                int maxDepth = initRecord.get("maxDepth").asInt();

                if (verbose) {
                    System.out.println("Initialized " + nodesProcessed + " nodes with depths (max depth: " + maxDepth + ")");
                    System.out.println("Set root " + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "] "
                            + idName + " = " + rootId);
                }

                // Step 2: Process nodes level by level iteratively
                // With reversed arrows, children point TO parents with incoming arrows from parent's perspective
                for (int currentLevel = 1; currentLevel <= maxDepth; currentLevel++) {
                    var levelProcessQuery = """
                MATCH (root)
                WHERE $nodeMemberLabel IN labels(root)
                  AND root[$idParamName] = $idParamValue

                // Find parents of this level from the subtree
                MATCH p=(root)<-[*0..]-(parent)
                WHERE $nodeMemberLabel IN labels(parent)
                  AND parent.depth = $parentDepth
                  AND parent[$idName] IS NOT NULL

                // Ensure parent has children of the same label
                // With reversed arrows, children point TO parents, so parent has incoming arrows
                WITH DISTINCT parent
                WHERE EXISTS {
                  MATCH (parent)<-[]-(child)
                  WHERE $nodeMemberLabel IN labels(child)
                }

                WITH parent,
                     (10.0 ^ -toFloat(parent.depth + 1)) AS next_place,
                     [ (parent)<-[]-(child)
                       WHERE $nodeMemberLabel IN labels(child) | child ] AS children,
                     toFloat(parent.depth + 1) AS child_depth

                // Precompute rounding factor for this level
                WITH parent, next_place, children, child_depth,
                     10.0 ^ child_depth AS rounding_factor

                UNWIND range(0, size(children) - 1) AS childIndex
                WITH parent, next_place, children, childIndex, child_depth, rounding_factor,
                     children[childIndex] AS currentChild,
                     reduce(total_offset = 0.0,
                        prev_idx IN range(0, childIndex - 1) |
                        total_offset + (next_place * coalesce(children[prev_idx].interval_width, 0.0))
                     ) AS accumulated_offset

                // Calculate and round child's id to control error accumulation
                WITH currentChild,
                     parent[$idName] + next_place + accumulated_offset AS raw_id,
                     rounding_factor, $idName AS idNameParam

                SET currentChild[idNameParam] = toFloat(round(raw_id * rounding_factor) / rounding_factor)

                RETURN count(currentChild) AS childrenProcessed
            """;

                    Map<String, Object> levelParams = Map.of(
                            "idParamName", idParamName,
                            "idParamValue", idParamValue,
                            "nodeMemberLabel", nodeMemberLabel,
                            "parentDepth", currentLevel - 1,
                            "idName", idName
                    );

                    var levelResult = tx.run(levelProcessQuery, levelParams);

                    int childrenProcessed = 0;
                    if (levelResult.hasNext()) {
                        childrenProcessed = levelResult.stream()
                                .mapToInt(record -> record.get("childrenProcessed").asInt())
                                .sum();
                    }

                    if (verbose) {
                        if (childrenProcessed > 0) {
                            System.out.println("Level " + currentLevel + ": Assigned " + idName + " to " + childrenProcessed + " nodes");
                        } else if (currentLevel == 1) {
                            System.out.println("No children found at level 1 - tree may be a single node");
                        }
                    }
                }

                if (verbose) {
                    System.out.println("ID assignment processing completed for " + maxDepth + " levels");

                    // Step 3: Verify results by showing assigned id values
                    var verifyQuery = """
                        MATCH (root)
                        WHERE $nodeMemberLabel IN labels(root)
                          AND root[$idParamName] = $idParamValue
                        MATCH p=(root)<-[*0..]-(node)
                        WHERE $nodeMemberLabel IN labels(node)
                        WITH DISTINCT node
                        RETURN node[$idParamName] AS nodeKey,
                               node.depth AS depth,
                               node[$idName] AS nodeId,
                               node.interval_width AS intervalWidth
                        ORDER BY depth, nodeId
                    """;

                    Map<String, Object> verifyParams = Map.of(
                            "idParamName", idParamName,
                            "idParamValue", idParamValue,
                            "nodeMemberLabel", nodeMemberLabel,
                            "idName", idName
                    );

                    var verifyResult = tx.run(verifyQuery, verifyParams);

                    System.out.println("\n=== " + idName + " assignments for subtree rooted at "
                            + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "] ===");
                    System.out.printf("%-24s %-8s %-16s %-15s%n", "Node Key", "Depth", idName, "Interval Width");
                    System.out.println("-".repeat(70));

                    while (verifyResult.hasNext()) {
                        var record = verifyResult.next();
                        var nodeKey = record.get("nodeKey").asLong();
                        var depth = record.get("depth").asInt();
                        var nodeIdVal = record.get("nodeId").isNull() ? null : record.get("nodeId").asDouble();
                        var intervalWidth = record.get("intervalWidth").isNull() ? null : record.get("intervalWidth").asDouble();

                        System.out.printf("%-24s %-8d %-16s %-15s%n",
                                nodeKey,
                                depth,
                                nodeIdVal == null ? "null" : String.format("%.6f", nodeIdVal),
                                intervalWidth == null ? "null" : String.format("%.1f", intervalWidth));
                    }
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error processing ID assignment for root "
                    + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "]: " + e.getMessage());
            throw e;
        }
    }

}