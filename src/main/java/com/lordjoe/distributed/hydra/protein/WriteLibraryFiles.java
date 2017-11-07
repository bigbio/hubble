package com.lordjoe.distributed.hydra.protein;

import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.systemsbiology.xtandem.peptide.*;

/**
 * com.lordjoe.distributed.hydra.protein.WriteLibraryFiles
 * User: Steve
 * Date: 10/16/2014
 */
public class WriteLibraryFiles {

    public static final int NUMBER_PARTITIONS = 500;
    public static JavaPairRDD<Integer,WriteLibraryFiles> asLibraryFiles(JavaPairRDD<Integer,IPolypeptide> byMZ)
    {
         return byMZ.combineByKey((Function<IPolypeptide, WriteLibraryFiles>) v1 -> null,
                 (Function2<WriteLibraryFiles, IPolypeptide, WriteLibraryFiles>) (v1, v2) -> null
                 , (Function2<WriteLibraryFiles, WriteLibraryFiles, WriteLibraryFiles>) (v1, v2) -> null,
                 new Partitioner() {
                     @Override
                     public int numPartitions() {
                         return 0;
                     }

                     @Override
                     public int getPartition(final Object key) {
                         return (Integer)key % NUMBER_PARTITIONS;
                     }
                 }



         );
    }


}
