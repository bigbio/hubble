package com.lordjoe.distributed.util;

import com.lordjoe.distributed.*;

import java.io.*;
import java.util.*;

/**
 * com.lordjoe.distributed.util.IterableUtilities
 * User: Steve
 * Date: 8/28/2014
 */
public class IterableUtilities {


    public static <K> Iterable<K>  asIterable(K... inp)
    {
        List<K> holder = new ArrayList<>();
        Collections.addAll(holder, inp);

        return holder;
    }

    /**
     * convert an Iterable of KeyValueObjects into an iterable of values
     * @param itr non-null Iterable of key values
     * @param <V>  value type
     * @return non-null iterable
     */
    public static <V extends Serializable> Iterable<V>  asIterableValues(final Iterable<KeyValueObject> itr )
    {
        final Iterator<KeyValueObject> itrx = itr.iterator();
        Iterable<V> ret = () -> new Iterator<V>() {
             @Override public boolean hasNext() {
                return itrx.hasNext();
            }
            @Override public V next() {
                return (V)(itrx.next().value);
            }
             @Override public void remove() {
                throw new UnsupportedOperationException("Not Implemented");
            }
        };

        return ret;
    }



    public static void showListKeyValueResults(final ListKeyValueConsumer<String, Integer> pResults) {
        appendListKeyValueResults(pResults,System.out);
    }


    public static void appendListKeyValueResults(final ListKeyValueConsumer<String, Integer> pResults,Appendable out) {
        try {
            List<KeyValueObject<String, Integer>> list = pResults.getList();
            list.sort(KeyValueObject.KEY_COMPARATOR);

            for (KeyValueObject<String, Integer> kv : list) {
                out.append(kv.toString());
                out.append("\n");
             }
        }
        catch (Exception e) {
            throw new RuntimeException(e);

        }
    }


}
