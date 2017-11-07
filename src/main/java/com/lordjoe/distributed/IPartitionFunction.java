package com.lordjoe.distributed;

import java.io.*;

/**
 * com.lordjoe.distributed.IPartitionFunction
 * User: Steve
 * Date: 8/29/2014
 */
public interface IPartitionFunction<K> extends Serializable {

    /**
     * default implementation use hash code
     */
    IPartitionFunction  HASH_PARTITION = (IPartitionFunction) inp -> {
        int ret = inp.hashCode();
        if(ret < 0)
            ret = -ret; // % does not work on negative numbers
        return ret;
    };

    int getPartition(K inp);
}
