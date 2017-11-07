package com.lordjoe.distributed;

import javax.annotation.*;
import java.util.*;

/**
 *  com.lordjoe.distributed.NullStringMapper
 *  effectively an identity mapper
 * User: Steve
 * Date: 8/28/2014
 */
public class NullStringMapper implements IMapperFunction<String,String, String, String> {


    public NullStringMapper() {
    }

    /**
     * this is what a Mapper does
     *
     * @param keyin
     * @param valuein
     * @return iterator over mapped key values
     */
    @Nonnull @Override public Iterable<KeyValueObject<String, String>> mapValues(@Nonnull final String key,@Nonnull final String line) {
           List<KeyValueObject<String, String>> holder = new ArrayList<>();
           holder.add(new KeyValueObject<>(key, line));
             return holder;
    }

}
