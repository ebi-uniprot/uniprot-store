package org.uniprot.store.spark.indexer.publication.mapper;

import static org.uniprot.store.spark.indexer.publication.MappedReferenceRDDReader.*;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.publication.MappedReference;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 26/03/2021
 */
public class MappedReferencePairMapper
        implements PairFunction<MappedReference, String, MappedReference> {

    private final KeyType keyType;
    private static final long serialVersionUID = 6452187369366968229L;

    public MappedReferencePairMapper(KeyType keyType) {
        this.keyType = keyType;
    }

    @Override
    public Tuple2<String, MappedReference> call(MappedReference mappedReference) throws Exception {
        return new Tuple2<>(getTupleKey(mappedReference), mappedReference);
    }

    private String getTupleKey(MappedReference ref) {
        if (keyType == KeyType.ACCESSION_AND_CITATION_ID) {
            return ref.getUniProtKBAccession().getValue() + "_" + ref.getCitationId();
        } else {
            return ref.getCitationId();
        }
    }
}
