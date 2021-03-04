// package discover.model;
//
// import java.util.ArrayList;
// import java.util.List;
// import java.util.Queue;
// import java.util.LinkedList;
//
// public class TupleSetTree {
//
//     // The node class.
//     public static class Node {
//         private TupleSet tupleSet;
//         private Node parent;
//         private List<Node> children;
//     }
//
//     private Node root; // The root of the tree.
//
//     public TupleSetTree(TupleSet tupleSet) {
//         this.root = new Node();
//         this.root.tupleSet = tupleSet;
//         this.root.children = new ArrayList<Node>();
//     }
//
//     public TupleSetTree() {}
//
//     // Returns the root of the tree.
//     public TupleSet getRootData() {
//         return this.root.tupleSet;
//     }
//
//     // Performs a Breadth First traversal of the tree and returns a list
//     // of the adjacent tuple sets of every node (of the tree) in the graph.
//     // If a node does not have any adjacent tuple sets it is not added in the list.
//     public List<AdjacentTupleSets> getAdjacentTupleSets(TupleSetGraph tupleSetGraph) {
//         if (this.root == null) return null;
//
//         Queue<Node> queue = new LinkedList<Node>(); // For the breadth first traversal.
//         List<AdjacentTupleSets> adjacent = new ArrayList<AdjacentTupleSets>();
//
//         queue.add(this.root);
//         while (!queue.isEmpty()) {
//             Node node = queue.remove();
//             // Get the adjacent tuple sets of the current node.
//             AdjacentTupleSets adjacentOfCurrentNode = tupleSetGraph.getAdjacentTupleSets(node);
//             if (adjacentOfCurrentNode != null) {
//                 adjacent.add(adjacentOfCurrentNode);
//             }
//
//             for (Node child: node.children) {
//                 queue.add(child);
//             }
//         }
//
//         return adjacent;
//     }
//
// }
