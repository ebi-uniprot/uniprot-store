package org.uniprot.store.spark.indexer.literature.mapper;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.store.spark.indexer.common.converter.UniProtKBReferencesConverter;

import scala.Tuple2;

/**
 * Maps an entry string to an iterator of tuples with values <citationId, UniProtKBReference>.
 *
 * @author lgonzales
 * @since 24/03/2021
 */
public class LiteratureUniProtKBReferencesMapper
        implements PairFlatMapFunction<String, String, UniProtKBReference> {

    private static final long serialVersionUID = -3292552212361568704L;
    private final UniProtKBReferencesConverter uniProtKBReferencesConverter =
            new UniProtKBReferencesConverter();

    @Override
    public Iterator<Tuple2<String, UniProtKBReference>> call(String entryStr) throws Exception {
        String[] lines = entryStr.split("\n");
        List<UniProtKBReference> references = uniProtKBReferencesConverter.convert(lines);
        return references.stream()
                .map(reference -> new Tuple2<>(reference.getCitation().getId(), reference))
                .iterator();
    }
}
