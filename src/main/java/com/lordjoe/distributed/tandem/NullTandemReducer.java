package com.lordjoe.distributed.tandem;

import com.lordjoe.distributed.*;
import org.systemsbiology.xtandem.*;

import javax.annotation.*;

/**
 * com.lordjoe.distributed.tandem.NullTandemMapper
 * User: Steve
 * Date: 10/1/2014
 */
public class NullTandemReducer extends AbstractTandemFunction  implements IReducerFunction<String, String,String, String> {
    public NullTandemReducer(final XTandemMain pMain) {
        super(pMain);
    }

    /**
     * this is what a reducer does
     *
     * @param key
     * @param values
     * @param consumer @return iterator over mapped key values
     */
    @Nonnull
    @Override
    public void handleValues(@Nonnull final String key, @Nonnull final Iterable<String> values, final IKeyValueConsumer<String, String>... consumer) {
        for (IKeyValueConsumer<String, String> aConsumer : consumer) {
            for (String value : values) {
                aConsumer.consume(new KeyValueObject<>(key, value));
            }
        }
     }
}
