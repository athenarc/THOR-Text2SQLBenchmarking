package shared.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

// A generic tree data structure.
public class Tree<T> implements Serializable {

    private static final long serialVersionUID = 5169625440500850478L;

    // The node class.
    public static class GenericNode<T> implements Serializable {
        private static final long serialVersionUID = -8053543237392367308L;
        private T data;
        private GenericNode<T> parent;
        private List<GenericNode<T>> children;

        // Public constructors
        public GenericNode(T data) {            
            this.data = data;
            this.parent = null;
            this.children = new ArrayList<>();
        }
    

        /** Getters */
        public void setChildren(List<GenericNode<T>> children) { this.children = children; }        
        public void setParent(GenericNode<T> parent) { this.parent = parent; }
        public void setData(T data) { this.data = data; }        
        
        /** Setters */
        public List<GenericNode<T>> getChildren() { return children; }        
        public GenericNode<T> getParent() { return parent; }
        public T getData() { return data; }        

        /**
         * Add a child on the node's children and update child's parent.
         * @param child
         */
        public void addChild(GenericNode<T> child) {
            this.children.add(child);
            child.parent = this;
        }

        @Override
        public String toString() {
            return this.data.toString();
        }

    }

    private GenericNode<T> root;                           // The root of the tree.
    private HashMap<T, GenericNode<T>> dataToGenericNodeMapping;  // A mapping between data and Actual Tree Nodes.

    /** Public constructor. */
    public Tree(T rootData) {
        this.dataToGenericNodeMapping = new HashMap<>();
        createGenericNode(rootData);
        this.root = this.dataToGenericNodeMapping.get(rootData);
    }

    public Tree() {
        this.dataToGenericNodeMapping = new HashMap<>();
    }

    /**
     * @param node The new generic node's data.
     */
    public void createGenericNode(T nodeData) {
        GenericNode<T> actualNode = new GenericNode<T>(nodeData);
        this.dataToGenericNodeMapping.put(nodeData, actualNode);
    }


    /**
     * Add a child to the Root Node of the Tree.
     * 
     * @param child The Child to Add.
     */
    public void addChildToRoot(T child) {
        // If the Child is not in the Tree return.
        if (!this.dataToGenericNodeMapping.containsKey(child)) return;
        GenericNode<T> genChild = this.dataToGenericNodeMapping.get(child);

        // Add the child to the parent's children.
        this.root.addChild(genChild);
    }

    /**
     * Adds a child to the parent Node. The parent node must be already in the Tree.     
     *      
     * @param parent The parent Node.
     * @param child The child Node.
     */
    public void addChild(T parent, T child) {
        // If the parent is not in the Tree then return.
        if (!this.dataToGenericNodeMapping.containsKey(parent)) return;
        GenericNode<T> genParent = this.dataToGenericNodeMapping.get(parent);
        
        // If the Child is not in the Tree Create return.
        if (!this.dataToGenericNodeMapping.containsKey(child)) return;
        GenericNode<T> genChild = this.dataToGenericNodeMapping.get(child);

        // Add the child to the parent's children.
        genParent.addChild(genChild);
    }

    /**
     * Set the param child to the param order in it's parent's child array.
     * 
     * @param child The child
     * @param order The order
     */
    public void setChildOrder(T child, int order) {
        // If the Child is not in the Tree return.
        if (!this.dataToGenericNodeMapping.containsKey(child)) return;
        GenericNode<T> genChild = this.dataToGenericNodeMapping.get(child);

        // if parent is null then return
        if (genChild.parent == null) return;

        // Put it in the param order
        genChild.parent.children.set(order, genChild);
    }


    /**
     * Delete the parameter Node from the Tree. If node has children then 
     * add them to it's parent's children.
     * 
     * @param node
     */
    public void deleteNode(T node) {
        // If the node is not in the Tree then return.
        if (!this.dataToGenericNodeMapping.containsKey(node)) return;
        GenericNode<T> genNode = this.dataToGenericNodeMapping.get(node);

        // if he has no father then delete it along with its children.
        if (genNode.parent == null) {            
            for (GenericNode<T> child: genNode.getChildren())
                this.dataToGenericNodeMapping.remove(child.data);            
        }
        else {
            // Remove the node form its parent's children List.
            genNode.parent.children.remove(genNode);

            // Add the nodes children to its parent's children List.
            if (!genNode.children.isEmpty())
                for (GenericNode<T> child: genNode.children) {                    
                    genNode.parent.children.add(child);
                    child.parent = genNode.parent;
                }
            
        }

        // Remove the node from the Data to GenNode Mapping.
        this.dataToGenericNodeMapping.remove(genNode.data);
    }


    /**
     * Makes the newParent parent of the child node.
     * 
     * @param newParent The new parent of the param child
     * @param child The node whose parent is goind to be the newParent param
     */
    public void makeParent(T newParent, T child) {
        // If the parent is not in the Tree then return.
        if (!this.dataToGenericNodeMapping.containsKey(newParent)) return;
        GenericNode<T> genParent = this.dataToGenericNodeMapping.get(newParent);
        
        // If the Child is not in the Tree then return.
        if (!this.dataToGenericNodeMapping.containsKey(child)) return;
        GenericNode<T> genChild = this.dataToGenericNodeMapping.get(child);

        GenericNode<T> parentsParent = genChild.parent;
        // If child is the root the make new Parent the new root
        if (genChild == this.root) 
            this.root = genParent;
        else
            parentsParent.children.remove(genChild);  // Remove child from parent's children.
                    
        genParent.addChild(genChild);       // Make genChild child of the new parent  
        parentsParent.addChild(genParent);  // Make genParent parent of the child's previous father.
    }


    /**
     * @param node The node to return its children.
     * @return Return the children of the Node. If the node is not in the tree Return null.
     */
    public List<T> getChildren(T node) {
        // If the node is not in the Tree then return.
        if (!this.dataToGenericNodeMapping.containsKey(node)) return null;
        GenericNode<T> genNode = this.dataToGenericNodeMapping.get(node);

        // Return the node's children
        List<T> children = new ArrayList<>();
        for (GenericNode<T> child: genNode.children) 
            children.add(child.data);

        return children;
    }

    /**
     * Remove the param childToRemove from the children of the param parent.
     * 
     * @param parent The parent
     * @param childToRemove The child
     * @return A boolean indicating if the removal was successful.
     */
    public boolean removeChild(T parent, T childToRemove) {
        // If the parent or the child is not in the Tree then return.
        if (
            !this.dataToGenericNodeMapping.containsKey(parent) ||
            !this.dataToGenericNodeMapping.containsKey(childToRemove) 
          ) 
            return false;

        // Get the generic Nodes.
        GenericNode<T> genParent = this.dataToGenericNodeMapping.get(parent);
        GenericNode<T> genChild = this.dataToGenericNodeMapping.get(childToRemove);

        // Remove the child from parent node. (if it exists)
        return genParent.children.remove(genChild);        
    }


    /**
     * Clear all children from Node. The nodes remain in the tree!
     * 
     * @param node The parent.     
     */
    public void clearAllChildren(T node) {
        // If the parent or the child is not in the Tree then return.
        if ( !this.dataToGenericNodeMapping.containsKey(node) ) 
            return;

        // Get the generic Node.
        GenericNode<T> genNode = this.dataToGenericNodeMapping.get(node);

        // Clear all the node's children.
        genNode.children.clear();
    }



    /**
     * Remove the param child from it's parent in the tree structure.
     *      
     * @param child The child
     * @return A boolean indicating if the removal was successful.
     */
    public boolean removeParent(T child) {
        // If the parent or the child is not in the Tree then return.
        if (!this.dataToGenericNodeMapping.containsKey(child) ) 
            return false;

        // Get the generic Node.        
        GenericNode<T> genChild = this.dataToGenericNodeMapping.get(child);        

        // Remove the child from parent node. (if it exists)
        if (genChild.parent == null)
            return false;
        else {
            boolean rv = genChild.parent.children.remove(genChild);        
            genChild.parent = null;
            return rv;
        }
    }

    /**
     * @param node The node to return its parent.
     * @return Return the Parent of the Node. If the node is not in the tree Return null.
     */
    public T getParent(T node) {
        // If the node is not in the Tree then return.
        if (!this.dataToGenericNodeMapping.containsKey(node)) return null;
        GenericNode<T> genNode = this.dataToGenericNodeMapping.get(node);

        // Return the node's children
        if (genNode.parent == null)
            return null;
        else 
            return genNode.parent.data;
    }  



    /**
     * @return The root of the Tree (The Data of the Generic Root Node).
     */
    public T getRoot() {
        return this.root.data;
    }

    /**
     * @param root the root to set
     */
    public void setRoot(T rootData) {
        createGenericNode(rootData);
        this.root = this.dataToGenericNodeMapping.get(rootData);        
    }


    public List<T> getAllNodes() {
        return new ArrayList<>(this.dataToGenericNodeMapping.keySet());
    }
     

    /**
     * Print the Tree. 
     * Ex : 
     *    <Tree>      |  <Print>
     *      a         |  a :
     *    /  \        |    b :
     *   b    c       |      d :
     *  /             |    c :
     * d              |
     * 
     */
    @Override
    public String toString() {
        Stack<Pair<GenericNode<T>, Integer>> stack = new Stack<>(); // For the depth first traversal.
        String str = new String();

        // Add the Root to the Queue.
        Integer depth = 0;
        stack.add(new Pair<GenericNode<T>, Integer>(this.root, depth));

        // Loop all the tree Nodes.
        while (!stack.isEmpty()) {
            Pair<GenericNode<T>, Integer> pair = stack.pop();
            GenericNode<T> genNode = pair.getLeft();
            depth = pair.getRight();

            str += PrintingUtils.addPrefixCharNumTimesToStr(
                depth,
                genNode.toString(),
                "    "
            );

            for (GenericNode<T> child : genNode.children) {
                stack.add(new Pair<GenericNode<T>, Integer>(child, depth + 1));
            }

            str += "\n";
        }

        return str;
    }
 

     /**
     * Make this Tree a clone of the parameter tree. If the actual 
     * data of the Tree will be cloned is up to the Type of T.
     * 
     * @param tree graph to clone
     * @param fillDataToClonedData a maps between param graph's data and this graph's cloned data. if its not null fill it.
     */        
    public void cloneLikeTree(Tree<T> tree, HashMap<T, T> fillDataToClonedData) {
        Queue<GenericNode<T>> queue = new LinkedList<>();                         // A queue to traverse the tree
        if (fillDataToClonedData == null) 
            fillDataToClonedData = new HashMap<>();

        // Clone all the tree's nodes first.
        for (T data: tree.getAllNodes()) {
            T clonedData = this.cloneData(data);  // Clone Tree's Data node.
            this.createGenericNode(clonedData);   // Create the generic node in this tree.
            fillDataToClonedData.put(data, clonedData);  // Update the map.
        }

        // Traverse the tree and create new Nodes filling the map
        queue.add(tree.root);
        this.setRoot( fillDataToClonedData.get(tree.root.data));  // Set the root.

        while (!queue.isEmpty()) {
            GenericNode<T> treeParent = queue.poll();
            GenericNode<T> thisParent = this.dataToGenericNodeMapping.get( fillDataToClonedData.get(treeParent.data) );

            // Copy the children from treeParent to thisParent using the fillDataToCloneData map
            for (GenericNode<T> child: treeParent.children) {
                queue.add(child);
                this.addChild(thisParent.data, fillDataToClonedData.get(child.data));
            }
        }
    }


    /** 
     * Return the Clone of the data if it is possible. Else 
     * return the same object.
     * 
     * @param data the data to clone.
     * @return data.clone() or data depending on the type of data.
     */
    @SuppressWarnings("unchecked")
    private T cloneData(T data) {
        T clonedData = null;
        try {
            clonedData = (T) data.getClass().getMethod("clone").invoke(data);
        } catch (Exception e) {
            clonedData = data;
        }
        return clonedData;
    }

}
