package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;

import scala.Serializable;
import scala.Tuple2;

/**
 * This class is Responsible to Map a UniParcEntry, to a Tuple{key=accession, value=uniParcId}
 *
 * @author lgonzales
 * @since 24/06/2020
 */
public class UniParcJoinMapper
        implements PairFlatMapFunction<UniParcEntry, String, String>, Serializable {
    private static final long serialVersionUID = 6710333532716208429L;

    @Override
    public Iterator<Tuple2<String, String>> call(UniParcEntry uniParcEntry) throws Exception {
        List<Tuple2<String, String>> mappedIds = new ArrayList<>();
        String uniParcId = uniParcEntry.getUniParcId().getValue();
        uniParcEntry.getUniParcCrossReferences().stream()
                .filter(this::filterActiveUniprotKBAndTrembl)
                .map(reference -> mapToTuple(uniParcId, reference))
                .forEach(mappedIds::add);
        return mappedIds.iterator();
    }

    private Tuple2<String, String> mapToTuple(String uniParcId, UniParcCrossReference reference) {
        return new Tuple2<>(reference.getId(), uniParcId);
    }

    private boolean filterActiveUniprotKBAndTrembl(UniParcCrossReference uniParcCrossReference) {
        return uniParcCrossReference.isActive()
                && (uniParcCrossReference.getDatabase().equals(UniParcDatabase.TREMBL)
                        || uniParcCrossReference.getDatabase().equals(UniParcDatabase.SWISSPROT)
                        || uniParcCrossReference
                                .getDatabase()
                                .equals(UniParcDatabase.SWISSPROT_VARSPLIC));
    }
}
