package org.uniprot.store.spark.indexer.publication.mapper;

import static org.uniprot.store.spark.indexer.publication.MappedReferenceRDDReader.*;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.uniprotkb.UniProtKBAccession;

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
        if (ref == null) {
            throw new IllegalStateException("Unable to create publication mapped-reference key");
        }
        String citationId = ref.getCitationId();
        if (citationId == null || citationId.isEmpty()) {
            throw new IllegalStateException(
                    "Unable to create publication mapped-reference key. Missing citationId. referenceType="
                            + ref.getClass().getName());
        }
        if (keyType == KeyType.ACCESSION_AND_CITATION_ID) {
            UniProtKBAccession accession = ref.getUniProtKBAccession();
            String accessionValue = accession == null ? null : accession.getValue();
            if (accessionValue == null || accessionValue.isEmpty()) {
                throw new IllegalStateException(
                        "Unable to create publication mapped-reference key. Missing accession. referenceType="
                                + ref.getClass().getName()
                                + ", citationId="
                                + citationId);
            }
            return accessionValue + "_" + citationId;
        } else {
            return citationId;
        }
    }
}
