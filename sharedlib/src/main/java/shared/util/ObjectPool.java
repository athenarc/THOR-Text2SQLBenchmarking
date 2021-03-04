package shared.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An implementation of an ObjectPool.
 * There is no expatriation time, and only the class Type T will define when 
 * the object needs to be removed from the pool
 * 
 * @NOTE url: 'https://sourcemaking.com/design_patterns/object_pool/java'
 */
public abstract class ObjectPool<T> {

    protected CopyOnWriteArrayList<T> locked, unlocked;
    private int capacity;
  
    public ObjectPool(int capacity) {      
        this.locked = new CopyOnWriteArrayList<T>();
        this.unlocked = new CopyOnWriteArrayList<T>();
        this.capacity = capacity;
    }

    public ObjectPool(int capacity, Collection<T> objects) {      
        this.locked = new CopyOnWriteArrayList<T>();
        this.unlocked = new CopyOnWriteArrayList<T>();
        this.capacity = capacity;

        // Add the items from collection.
        int count = 0;
        for (T object: objects) {
            unlocked.add(object);            
            if ( ++count > capacity)
                break; 
        }
    }
   

    /**
     * Returns a pooled object even if this means that we have to wait.
     */
    public T reserve_blocking(){
        // Loop until you get an object of type T
        T t = null;
        while ( (t = this.reserve_try()) == null );
        return t;
    }

    /**
     * Get an available object. If none then return null.
     */
    public synchronized T reserve_try() {
        T ret_t = null;

        // Find if there are unlocked items
        if (unlocked.size() > 0) {
            // ArrayList<T> toDelete = new ArrayList<>();
            Iterator<T> iter = unlocked.iterator();
            while (iter.hasNext()) {
                T t = iter.next();
                if (validate(t)) {
                    unlocked.remove(t);
                    locked.add(t);
                    return t;
                } else {
                    // object failed validation
                    unlocked.remove(t);                    
                }                
            }
        }

        // See if we can create more objects
        if ( capacity > locked.size() + unlocked.size() ) {
            ret_t = create();
            locked.add(ret_t);
        }

        return ret_t;
    }

    /**
     * Return an object back to the bool
     */
    public synchronized void release(T t) {
        if (locked.remove(t))
            unlocked.add(t);
    }
        

    // FUNCTIONS TO IMPLEMENT BY SUB CLASSES        
    protected abstract T create();
    public abstract boolean validate(T o);    

    /**
     * This function defines whether to expand the Pool by adding a new object or 
     * just block until an object is released. This function gets called every time 
     * you call reserve and no object is available.
     */
    // public abstract boolean expansion_policy();
}