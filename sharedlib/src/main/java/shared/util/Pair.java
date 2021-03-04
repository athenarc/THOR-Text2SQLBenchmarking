package shared.util;

public class Pair<D1, D2> {

    private D1 left;
    private D2 right;

    public Pair(D1 left, D2 right) {
        this.left = left;
        this.right = right;
    }

    public Pair(Pair<D1, D2> pair) {
        this.left = pair.left;
        this.right = pair.right;
    }

    // Getters and Setters.
    public D1 getLeft() {
        return left;
    }
    
    public void setLeft(D1 left) {
        this.left = left;
    }

    public D2 getRight() {
        return right;
    }

    public void setRight(D2 right) {
        this.right = right;
    }

    // Returns true if left or right is null
    public boolean containsNullObject() {
        return this.left == null || this.right == null;
    }

}
