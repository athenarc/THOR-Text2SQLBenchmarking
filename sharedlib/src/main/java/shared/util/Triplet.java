package shared.util;

public class Triplet<D1, D2, D3> {

    private D1 first;
    private D2 second;
    private D3 third;

    public Triplet(D1 first, D2 second, D3 third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public Triplet(Triplet<D1, D2, D3> triplet) {
        this.first = triplet.first;
        this.second = triplet.second;
        this.third = triplet.third;
    }

    // Getters and Setters.    
    public D1 getFirst() {
        return first;
    }

    public void setFirst(D1 first) {
        this.first = first;
    }

        
    public D2 getSecond() {
        return second;
    }
    
    public void setSecond(D2 second) {
        this.second = second;
    }
    

    public D3 getThird() {
        return third;
    }

    public void setThird(D3 third) {
        this.third = third;
    }

    // Returns true if left or right is null
    public boolean containsNullObject() {
        return this.first == null || this.second == null || this.third == null;
    }
        

}
