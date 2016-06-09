package org.deeplearning4j;

import org.apache.kafka.common.serialization.Deserializer;
import org.canova.api.writable.Writable;
import org.deeplearning4j.util.SerializationUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Map;

/**
 * Created by agibsonccc on 6/7/16.
 */
public class RecordDeSerializer implements Deserializer<Collection<Collection<Writable>>> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Collection<Collection<Writable>> deserialize(String s, byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        BufferedInputStream bis2 = new BufferedInputStream(bis);
        return SerializationUtils.readObject(bis2);
    }

    @Override
    public void close() {

    }
}
