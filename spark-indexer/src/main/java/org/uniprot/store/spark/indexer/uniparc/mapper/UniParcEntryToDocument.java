package org.uniprot.store.spark.indexer.uniparc.mapper;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.search.document.uniparc.UniParcDocument;
import org.uniprot.store.search.document.uniparc.UniParcDocumentConverter;

import scala.Tuple2;

/**
 * This class get an UniParcEntry and convert to UniParcDocument and returns a Tuple{key=uniparcId,
 * value=UniParcDocument} so this tuple can be Joined
 *
 * @author lgonzales
 * @since 2020-02-20
 */
public class UniParcEntryToDocument implements PairFunction<UniParcEntry, String, UniParcDocument> {
    private static final long serialVersionUID = 4491443919730778424L;

    @Override
    public Tuple2<String, UniParcDocument> call(UniParcEntry uniParcEntry) throws Exception {
        UniParcDocumentConverter converter = new UniParcDocumentConverter();
        UniParcDocument document = converter.convert(uniParcEntry);
        String uniparcId = uniParcEntry.getUniParcId().getValue();
        return new Tuple2<>(uniparcId, document);
    }
}
