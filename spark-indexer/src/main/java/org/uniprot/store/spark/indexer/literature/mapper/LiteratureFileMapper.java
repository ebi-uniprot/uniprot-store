package org.uniprot.store.spark.indexer.literature.mapper;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.citation.Literature;
import org.uniprot.store.reader.literature.LiteratureConverter;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 24/03/2021
 */
public class LiteratureFileMapper implements PairFunction<String, String, Literature> {

    private static final long serialVersionUID = 1131414443308173544L;

    @Override
    public Tuple2<String, Literature> call(String entryString) throws Exception {
        LiteratureConverter converter = new LiteratureConverter();
        Literature literature = converter.convert(entryString);
        return new Tuple2<>(literature.getId(), literature);
    }
}
