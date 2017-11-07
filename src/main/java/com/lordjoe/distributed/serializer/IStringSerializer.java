package com.lordjoe.distributed.serializer;

import javax.annotation.*;

/**
 * com.lordjoe.distributed.serializer.IStringSerializer
 * User: Steve
 * Date: 9/19/2014
 */
public interface IStringSerializer<T> {

    Class<? extends T>  getSerializedClass();

    /**
     * convert to string
     * @param t class
     * @return  string representation
     */
    @Nonnull
    String asString(@Nonnull T t);

    /**
     * build from a string - default may be to call constructor from string
     * @param t
     * @return
     */
    T fromString(@Nonnull String t);

}
