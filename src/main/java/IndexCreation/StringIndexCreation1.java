package IndexCreation;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;

import java.util.HashMap;
import java.util.Map;

public class StringIndexCreation1 implements AutoCloseable {
    private final Driver driver;
    private final String database;

    public StringIndexCreation1(String uri, String user, String password, String database) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        this.database = database;
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public static void main(String[] args) {
        // Connection details
        String uri = "neo4j://127.0.0.1:7687";  // Default Neo4j bolt port
        String user = "neo4j";
        String password = "giggleparabola";  // Change this to your actual password
        String database = "string.100k";

        /*
        try (StringIndexCreation1 app = new StringIndexCreation1(uri, user, password, database)) {

            app.complete_annotation(
                    "name",
                    1,
                    "TreeNode",
                    "1",
                    "float_id",
                    "HAS_CHILD",
                    false
            );

            // app.deleteData();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }

         */

        annotate_n_forest(database, 1001000, uri, user, password, 1);
    }

    public static void annotate_n_forest(String db_name, Integer tree_size, String uri, String user, String password, Integer n){
        try (StringIndexCreation1 app = new StringIndexCreation1(uri, user, password, db_name)) {
            Integer start_node_id = 1;
            Integer start_node_name = 1;

            for (int i=0; i<n; i++){
                app.complete_annotation(
                        "name",
                        start_node_name,
                        "TreeNode",
                        String.valueOf(start_node_id),
                        "float_id",
                        "HAS_CHILD",
                        false
                );


                start_node_id += 1;
                start_node_name += tree_size;
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
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

    private SessionConfig getSessionConfig() {
        return SessionConfig.builder()
                .withDatabase(database)
                .build();
    }

    // RELATIONSHIP AGNOSTIC
    public void complete_annotation(String idParamName,
                                    Integer idParamValue,
                                    String nodeMemberLabel,
                                    String rootId,
                                    String newIdName,
                                    String relType,
                                    boolean verbose
    ){
        annotateTreeTextIndex(
                idParamName,
                idParamValue,
                nodeMemberLabel,
                rootId,
                newIdName
        );
    }

    public void annotateTreeTextIndex(String idParamName,
                                      Integer idParamValue,
                                      String nodeMemberLabel,
                                      String rootId,
                                      String idName){
        try (Session session = driver.session(getSessionConfig())) {
            session.executeWrite(tx -> {
                // First, set the root node's ID
                String setRootQuery = String.format(
                        "MATCH (root) " +
                                "WHERE $nodeMemberLabel IN labels(root) " +
                                "  AND root[$idParamName] = $idParamValue " +
                                "SET root.%s = $rootIdStr " +
                                "RETURN root",
                        idName
                );

                Map<String, Object> rootParams = new HashMap<>();
                rootParams.put("nodeMemberLabel", nodeMemberLabel);
                rootParams.put("idParamName", idParamName);
                rootParams.put("idParamValue", idParamValue);
                rootParams.put("rootIdStr", rootId);

                tx.run(setRootQuery, rootParams);

                // Now iterate through each depth level and assign IDs
                // We use the depth property that was set by annotateDepthAndHeight
                String getMaxDepthQuery =
                        "MATCH (n) " +
                                "WHERE $nodeMemberLabel IN labels(n) " +
                                "  AND n.depth IS NOT NULL " +
                                "RETURN max(n.depth) as maxDepth";

                Map<String, Object> depthParams = new HashMap<>();
                depthParams.put("nodeMemberLabel", nodeMemberLabel);

                Result depthResult = tx.run(getMaxDepthQuery, depthParams);
                int maxDepth = 0;
                if (depthResult.hasNext()) {
                    Value maxDepthValue = depthResult.next().get("maxDepth");
                    if (!maxDepthValue.isNull()) {
                        maxDepth = maxDepthValue.asInt();
                    }
                }

                // Process each depth level starting from 1 (children of root)
                for (int depth = 1; depth <= maxDepth; depth++) {
                    String assignIdsQuery = String.format(
                            "MATCH (parent)<-[]-(child) " +
                                    "WHERE $nodeMemberLabel IN labels(parent) " +
                                    "  AND $nodeMemberLabel IN labels(child) " +
                                    "  AND child.depth = $currentDepth " +
                                    "  AND parent.%s IS NOT NULL " +
                                    "  AND child.%s IS NULL " +
                                    "WITH parent, child " +
                                    "ORDER BY parent, id(child) " +
                                    "WITH parent, collect(child) as children " +
                                    "UNWIND range(0, size(children)-1) as idx " +
                                    "WITH parent, children[idx] as child, idx " +
                                    "SET child.%s = parent.%s + '.' + toString(idx + 1)",
                            idName, idName, idName, idName
                    );

                    Map<String, Object> assignParams = new HashMap<>();
                    assignParams.put("nodeMemberLabel", nodeMemberLabel);
                    assignParams.put("currentDepth", depth);

                    tx.run(assignIdsQuery, assignParams);
                }

                return null;
            });

            System.out.println("Tree text index annotation completed successfully!");

        } catch (Neo4jException e) {
            System.err.println("Failed to annotate tree text index: " + e.getMessage());
            throw e;
        }
    }


}

