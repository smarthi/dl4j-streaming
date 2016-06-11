package org.deeplearning4j.streaming.conversion.dataset;

import org.canova.api.writable.Writable;
import org.nd4j.linalg.dataset.DataSet;

import java.util.Collection;

/**
 * Created by agibsonccc on 6/11/16.
 */
public interface RecordToDataSet {

    DataSet convert(Collection<Collection<Writable>> records, int numLabels);

}
