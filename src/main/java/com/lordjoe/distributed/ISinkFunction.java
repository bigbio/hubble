package com.lordjoe.distributed;

import java.io.*;
import java.nio.file.*;

/**
 * com.lordjoe.distributed.ISourceFunction
 * User: Steve
 * Date: 8/28/2014
 */
public interface ISinkFunction<K extends Serializable,V extends Serializable> {

    /**
     * null sink does nothing
     */
    ISinkFunction  NULL_FUNCTION = (p, vals) -> {
      };
    /**
     * null sink does nothing
     */
    ISinkFunction  PRINT_FUNCTION = (p, vals) -> {
        for (Object val : vals) {
            KeyValueObject kv = (KeyValueObject)val;
            System.out.println(kv.key + ":" + kv.value);
        }
      };
    /**
     * somehow define how a path is converted into an iterator
     * @param p
     * @return
     */
    void generateOutput(Path p, Iterable<KeyValueObject<K, V>> vals);
}
