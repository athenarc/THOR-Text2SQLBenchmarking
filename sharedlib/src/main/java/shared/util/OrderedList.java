package shared.util;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 */
public class OrderedList<D, S extends Comparable<S>> {

    // To test
    public static void main(String[] args) {
        OrderedList<String, Double> ol = new OrderedList<>(3);
        ol.addElement("Third", 5.0);
        ol.addElement("Forth", 4.0);
        ol.addElement("Second", 6.0);
        ol.addElement("First", 7.0);

        for (String s: ol.getElements())
            System.out.println(s);
        System.exit(0);    
    }



    public enum OrderType {Acceding, Descending};

    List<Pair<S, D>> elements;              // The elements.
    Integer numOfElements;                  // The max number of elements.
    OrderType type = OrderType.Descending;  // The type of ordering.
    
    /**
     * Create an ordered List, limiting the number of elements that it can contain.
     * Default Order Descending    
     * 
     * @param numOfElements The maximum number of elements that the list can contain.
     */
    public OrderedList(int numOfElements) {
        this.elements = new ArrayList<>();
        this.numOfElements = numOfElements;        
    }

    public OrderedList(int numOfElements, OrderType type) {
        this.elements = new ArrayList<>();
        this.numOfElements = numOfElements;
        this.type = type;
    }

    /** Ordered List constructor,  Default Order Descending */
    public OrderedList() {
        this.elements = new ArrayList<>();
        this.numOfElements = Integer.MAX_VALUE;        
    }
    public OrderedList(OrderType type) {
        this.elements = new ArrayList<>();
        this.numOfElements = Integer.MAX_VALUE;
        this.type = type;
    }


    /**    
     * @return The Elements stored int the Ordered List along with their scores.
     */
    public List<Pair<S, D>> getElementsWithScore() {
        return this.elements;
    }

    /**    
     * @return The Elements stored int the Ordered List along with their scores.
     */
    public List<D> getElements() {
        List<D> elems = new ArrayList<>();
        for (Pair<S, D> p: this.elements)
            elems.add(p.getRight());
        return elems;
    }
    
    /**
     * Ads an SQLQuery in string format inside the list along with its score.
     * Adding the SQLQuery means also sorting the list with the queries scores.
     * If the elements, after adding the sqlQuery parameter, are more than 
     * numOfElements then remove the one with the worst score.
     * 
     * @param pattern
     * @param score
     */
    public void addElement(D element, S score) {
        // If list is empty then add the element and return
        if (this.elements.isEmpty()) {
            elements.add(new Pair<>(score, element));          
            return;
        }

        // Loop the scores array and based on the Query's score
        // determine what is the right index to append the query.        
        boolean addedElement = false;
        for (int index = 0; index < this.elements.size(); index++) {
            if (this.type == OrderType.Descending) {
                if ( score.compareTo(this.elements.get(index).getLeft()) > 0 ) {
                    elements.add(index, new Pair<>(score, element));
                    addedElement = true;
                    break;
                }
            } else {
                if ( score.compareTo(this.elements.get(index).getLeft()) < 0 ) {
                    elements.add(index, new Pair<>(score, element));
                    addedElement = true;
                    break;
                }   
            }                                      
        }

        // If the list has more elements than numOfElements then remove the last one.
        if (this.elements.size() > this.numOfElements)
            this.elements.remove(this.elements.size() - 1);
        // Else if the list is not at full capacity and we didn't add the element, add it now.
        else if (this.elements.size() < this.numOfElements && addedElement == false)
            elements.add(new Pair<>(score, element));
    }


    /**
     * Returns the best Score. In case of acceding order the smallest 
     * and in case of descending order the biggest.
     * 
     * @return
     */
    public S peekScore() {
        if (this.elements.size() > 0)
            return this.elements.get(0).getLeft();
        else
            return null;
    }
}
