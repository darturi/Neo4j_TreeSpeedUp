package QueryAssessment;

import java.util.ArrayList;
import java.util.List;

public class GetQueryList {

        public static List<Neo4jQueryPerformanceTester.QueryTuple> getTreeTraversalQueries() {
            List<Neo4jQueryPerformanceTester.QueryTuple> queries = new ArrayList<>();

            // 1. All descendants of a node
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post {id: $root_id})<-[*]-(descendant) RETURN descendant",
                    "All descendants"
            ));

            // 2. Descendants at specific depth
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post {id: $root_id})<-[*3]-(descendant) RETURN descendant",
                    "Descendants at depth 3"
            ));

            // 3. Descendants within depth range
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post {id: $root_id})<-[*2..5]-(descendant) RETURN descendant",
                    "Descendants within depth range 2-5"
            ));

            // 4. All ancestors of a node
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (leaf:Comment {id: $leaf_id})-[*]->(ancestor) RETURN ancestor",
                    "All ancestors"
            ));

            // 5. All leaf nodes in subtree
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post {id: $root_id})<-[*]-(leaf) " +
                            "WHERE NOT (leaf)<-[]-() RETURN leaf",
                    "All leaf nodes in subtree"
            ));

            // 6. Subtree size (node count)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post {id: $root_id})<-[*]-(descendant) RETURN count(descendant) AS subtreeSize",
                    "Subtree size"
            ));

            // Replace Query 7 with:
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH path = (root:Post {id: $root_id})<-[*]-(leaf) " +
                            "WHERE NOT (leaf)<-[]-() RETURN max(length(path)) AS depth",
                    "Subtree depth/height"
            ));

            // Replace Query 8 with:
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH p1 = (node {id: $node_id})-[*]->(root) WHERE NOT (root)-[]->() " +
                            "WITH node, length(p1) AS nodeDepth, root " +
                            "MATCH p2 = (root)<-[*]-(peer) " +
                            "WHERE length(p2) = nodeDepth AND peer <> node RETURN peer",
                    "Nodes at same level"
            ));

            // 9. Children and grandchildren only
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post {id: $root_id})<-[*1..2]-(descendant) RETURN descendant",
                    "Children and grandchildren only"
            ));

            return queries;
        }

        public static List<Neo4jQueryPerformanceTester.QueryTuple> getIndexOptimizedQueries() {
            List<Neo4jQueryPerformanceTester.QueryTuple> queries = new ArrayList<>();

            // 1. All descendants of a node (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post:FloatIndexMember {id: $root_id}) " +
                            "MATCH (descendant:FloatIndexMember) " +
                            "WHERE descendant.new_id_label > root.new_id_label " +
                            "AND descendant.new_id_label < root.new_id_label + root.interval_width " +
                            "RETURN descendant",
                    "All descendants (index-optimized)"
            ));

            // 2. Descendants at specific depth (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post:FloatIndexMember {id: $root_id}) " +
                            "MATCH (descendant:FloatIndexMember) " +
                            "WHERE descendant.new_id_label > root.new_id_label " +
                            "AND descendant.new_id_label < root.new_id_label + root.interval_width " +
                            "AND descendant.depth = root.depth + 3 " +
                            "RETURN descendant",
                    "Descendants at depth 3 (index-optimized)"
            ));

            // 3. Descendants within depth range (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post:FloatIndexMember {id: $root_id}) " +
                            "MATCH (descendant:FloatIndexMember) " +
                            "WHERE descendant.new_id_label > root.new_id_label " +
                            "AND descendant.new_id_label < root.new_id_label + root.interval_width " +
                            "AND descendant.depth >= root.depth + 2 " +
                            "AND descendant.depth <= root.depth + 5 " +
                            "RETURN descendant",
                    "Descendants within depth range 2-5 (index-optimized)"
            ));

            // 4. All ancestors of a node (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (leaf:Comment:FloatIndexMember {id: $leaf_id}) " +
                            "MATCH (ancestor:FloatIndexMember) " +
                            "WHERE ancestor.new_id_label <= leaf.new_id_label " +
                            "AND leaf.new_id_label < ancestor.new_id_label + ancestor.interval_width " +
                            "AND ancestor.depth < leaf.depth " +
                            "RETURN ancestor",
                    "All ancestors (index-optimized)"
            ));

            // 5. All leaf nodes in subtree (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post:FloatIndexMember {id: $root_id}) " +
                            "MATCH (leaf:FloatIndexMember) " +
                            "WHERE leaf.new_id_label > root.new_id_label " +
                            "AND leaf.new_id_label < root.new_id_label + root.interval_width " +
                            "AND leaf.height = 0 " +
                            "RETURN leaf",
                    "All leaf nodes in subtree (index-optimized)"
            ));

            // 6. Subtree size (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post:FloatIndexMember {id: $root_id}) " +
                            "MATCH (descendant:FloatIndexMember) " +
                            "WHERE descendant.new_id_label > root.new_id_label " +
                            "AND descendant.new_id_label < root.new_id_label + root.interval_width " +
                            "RETURN count(descendant) AS subtreeSize",
                    "Subtree size (index-optimized)"
            ));

            // 7. Subtree depth/height (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post:FloatIndexMember {id: $root_id}) " +
                            "RETURN root.height AS depth",
                    "Subtree depth/height (index-optimized)"
            ));

            // 8. Nodes at same level from common root (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (node:Comment:FloatIndexMember {id: $node_id}) " +
                            "MATCH (root:Post:FloatIndexMember) " +
                            "WHERE root.new_id_label <= node.new_id_label " +
                            "AND node.new_id_label < root.new_id_label + root.interval_width " +
                            "WITH node, root " +
                            "MATCH (peer:FloatIndexMember) " +
                            "WHERE peer.new_id_label > root.new_id_label " +
                            "AND peer.new_id_label < root.new_id_label + root.interval_width " +
                            "AND peer.depth = node.depth " +
                            "AND peer <> node " +
                            "RETURN peer",
                    "Nodes at same level (index-optimized)"
            ));

            // 9. Children and grandchildren only (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:Post:FloatIndexMember {id: $root_id}) " +
                            "MATCH (descendant:FloatIndexMember) " +
                            "WHERE descendant.new_id_label > root.new_id_label " +
                            "AND descendant.new_id_label < root.new_id_label + root.interval_width " +
                            "AND descendant.depth >= root.depth + 1 " +
                            "AND descendant.depth <= root.depth + 2 " +
                            "RETURN descendant",
                    "Children and grandchildren only (index-optimized)"
            ));

            return queries;
        }

        public static List<Neo4jQueryPerformanceTester.QueryTuple> getFakeTreeTraversalQueries() {
            List<Neo4jQueryPerformanceTester.QueryTuple> queries = new ArrayList<>();

            // 1. All descendants of a node
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id})<-[*]-(descendant) RETURN descendant",
                    "All descendants"
            ));

            // 2. Descendants at specific depth
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id})<-[*3]-(descendant) RETURN descendant",
                    "Descendants at depth 3"
            ));

            // 3. Descendants within depth range
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id})<-[*2..5]-(descendant) RETURN descendant",
                    "Descendants within depth range 2-5"
            ));

            // 4. All ancestors of a node
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (leaf:TreeNode {name: $leaf_id})-[*]->(ancestor) RETURN ancestor",
                    "All ancestors"
            ));

            // 5. All leaf nodes in subtree
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id})<-[*]-(leaf) " +
                            "WHERE NOT (leaf)<-[]-() RETURN leaf",
                    "All leaf nodes in subtree"
            ));

            // 6. Subtree size (node count)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id})<-[*]-(descendant) RETURN count(descendant) AS subtreeSize",
                    "Subtree size"
            ));

            // Replace Query 7 with:
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH path = (root:TreeNode {name: $root_id})<-[*]-(leaf) " +
                            "WHERE NOT (leaf)<-[]-() RETURN max(length(path)) AS depth",
                    "Subtree depth/height"
            ));

            // Replace Query 8 with:
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH p1 = (node {name: $node_id})-[*]->(root) WHERE NOT (root)-[]->() " +
                            "WITH node, length(p1) AS nodeDepth, root " +
                            "MATCH p2 = (root)<-[*]-(peer) " +
                            "WHERE length(p2) = nodeDepth AND peer <> node RETURN peer",
                    "Nodes at same level"
            ));

            // 9. Children and grandchildren only
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id})<-[*1..2]-(descendant) RETURN descendant",
                    "Children and grandchildren only"
            ));

            return queries;
        }

        public static List<Neo4jQueryPerformanceTester.QueryTuple> getFakeIndexOptimizedQueries() {
            List<Neo4jQueryPerformanceTester.QueryTuple> queries = new ArrayList<>();

            // 1. All descendants of a node (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id}) " +
                            "MATCH (descendant:TreeNode) " +
                            "WHERE descendant.float_id > root.float_id " +
                            "AND descendant.float_id < root.float_id + root.interval_width " +
                            "RETURN descendant",
                    "All descendants (index-optimized)"
            ));

            // 2. Descendants at specific depth (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id}) " +
                            "MATCH (descendant:TreeNode) " +
                            "WHERE descendant.float_id > root.float_id " +
                            "AND descendant.float_id < root.float_id + root.interval_width " +
                            "AND descendant.depth = root.depth + 3 " +
                            "RETURN descendant",
                    "Descendants at depth 3 (index-optimized)"
            ));

            // 3. Descendants within depth range (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id}) " +
                            "MATCH (descendant:TreeNode) " +
                            "WHERE descendant.float_id > root.float_id " +
                            "AND descendant.float_id < root.float_id + root.interval_width " +
                            "AND descendant.depth >= root.depth + 2 " +
                            "AND descendant.depth <= root.depth + 5 " +
                            "RETURN descendant",
                    "Descendants within depth range 2-5 (index-optimized)"
            ));

            // 4. All ancestors of a node (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (leaf:TreeNode {name: $leaf_id}) " +
                            "MATCH (ancestor:TreeNode) " +
                            "WHERE ancestor.float_id <= leaf.float_id " +
                            "AND leaf.float_id < ancestor.float_id + ancestor.interval_width " +
                            "AND ancestor.depth < leaf.depth " +
                            "RETURN ancestor",
                    "All ancestors (index-optimized)"
            ));

            // 5. All leaf nodes in subtree (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id}) " +
                            "MATCH (leaf:TreeNode) " +
                            "WHERE leaf.float_id > root.float_id " +
                            "AND leaf.float_id < root.float_id + root.interval_width " +
                            "AND leaf.height = 0 " +
                            "RETURN leaf",
                    "All leaf nodes in subtree (index-optimized)"
            ));

            // 6. Subtree size (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id}) " +
                            "MATCH (descendant:TreeNode) " +
                            "WHERE descendant.float_id > root.float_id " +
                            "AND descendant.float_id < root.float_id + root.interval_width " +
                            "RETURN count(descendant) AS subtreeSize",
                    "Subtree size (index-optimized)"
            ));

            // 7. Subtree depth/height (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id}) " +
                            "RETURN root.height AS depth",
                    "Subtree depth/height (index-optimized)"
            ));

            // 8. Nodes at same level from common root (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (node:TreeNode {name: $node_id}) " +
                            "MATCH (root:TreeNode) " +
                            "WHERE root.float_id <= node.float_id " +
                            "AND node.float_id < root.float_id + root.interval_width " +
                            "WITH node, root " +
                            "MATCH (peer:TreeNode) " +
                            "WHERE peer.float_id > root.float_id " +
                            "AND peer.float_id < root.float_id + root.interval_width " +
                            "AND peer.depth = node.depth " +
                            "AND peer <> node " +
                            "RETURN peer",
                    "Nodes at same level (index-optimized)"
            ));

            // 9. Children and grandchildren only (INDEX)
            queries.add(new Neo4jQueryPerformanceTester.QueryTuple(
                    "MATCH (root:TreeNode {name: $root_id}) " +
                            "MATCH (descendant:TreeNode) " +
                            "WHERE descendant.float_id > root.float_id " +
                            "AND descendant.float_id < root.float_id + root.interval_width " +
                            "AND descendant.depth >= root.depth + 1 " +
                            "AND descendant.depth <= root.depth + 2 " +
                            "RETURN descendant",
                    "Children and grandchildren only (index-optimized)"
            ));

            return queries;
        }
}
