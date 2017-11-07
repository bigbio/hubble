package com.lordjoe.distributed;

import java.nio.file.*;

/**
 * com.lordjoe.distributed.ISourceFunction
 * User: Steve
 * Date: 8/28/2014
 */
public interface ISourceFunction<K> {

    /**
     * somehow define how a path is converted into an iterator
     * @param p
     * @return
     */
    Iterable<K> readInput(Path p);
}
