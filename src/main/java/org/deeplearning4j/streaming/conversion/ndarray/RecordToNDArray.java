package org.deeplearning4j.streaming.conversion.ndarray;

import org.canova.api.writable.Writable;
import org.nd4j.linalg.api.ndarray.INDArray;

import java.util.Collection;

/**
 * Created by agibsonccc on 6/11/16.
 */
public interface RecordToNDArray  {

    INDArray convert(Collection<Collection<Writable>> records);

}
