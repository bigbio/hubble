package com.lordjoe.algorithms;

import java.io.Serializable;
import java.util.*;

/**
 * Map containing a list of items
 *
 * @param <K>  key type
 * @param <V>  value type
 */
public class MapOfLists<K,V> implements Serializable {
    private Map<K, List<V>> items = new HashMap<>();

    public MapOfLists() {
    }


    public Set<K>  keySet() {
        return items.keySet();
    }

    public Collection<List<V>>  values() {
        return items.values();
    }

    public int size() {
        return items.size();
    }

   public List<V> get(K key)  {
       return items.get(key);
   }

    public void putItem(K key,V value)  {
        if(items.containsKey(key))   {
            items.get(key).add(value);
        }
        else {
            List<V>  added = new ArrayList<>();
            added.add(value);
            items.put(key,added);
        }
    }
    public void putItems(K key,List<V> value)  {
         if(items.containsKey(key))   {
             items.get(key).addAll(value);
         }
         else {
             List<V>  added = new ArrayList<>();
             added.addAll(value);
             items.put(key,added);
         }
     }


    public boolean containsKey(K k)
    {
         return items.containsKey(k);
    }

    public boolean containsEntryGreaterThanOne()
     {
         for (List<V> vs : items.values()) {
             if(vs.size() > 1)
                 return true;
         }
         return false;
     }

}
