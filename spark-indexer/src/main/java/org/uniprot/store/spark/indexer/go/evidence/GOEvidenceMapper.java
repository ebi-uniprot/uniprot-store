package org.uniprot.store.spark.indexer.go.evidence;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprotkb.UniProtkbEntry;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.impl.UniProtkbEntryBuilder;
import org.uniprot.core.uniprotkb.xdb.UniProtkbCrossReference;
import org.uniprot.core.uniprotkb.xdb.impl.UniProtCrossReferenceBuilder;

import scala.Tuple2;

/**
 * This class is responsible to map an Iterable of GOEvidence to an UniProtkbEntry
 *
 * @author lgonzales
 * @since 2019-10-22
 */
public class GOEvidenceMapper
        implements Function<
                Tuple2<UniProtkbEntry, Optional<Iterable<GOEvidence>>>, UniProtkbEntry> {

    private static final long serialVersionUID = 7478726902589041984L;

    /**
     * @param tuple of <UniProtkbEntry, Iterable of GoEvidence>
     * @return UniProtkbEntry with extra Go Evidences
     */
    @Override
    public UniProtkbEntry call(Tuple2<UniProtkbEntry, Optional<Iterable<GOEvidence>>> tuple)
            throws Exception {
        UniProtkbEntryBuilder entry = UniProtkbEntryBuilder.from(tuple._1);
        if (tuple._2.isPresent()) {
            Map<String, List<Evidence>> goEvidenceMap = getGoEvidenceMap(tuple._2.get());

            List<UniProtkbCrossReference> xrefs =
                    tuple._1.getUniProtkbCrossReferences().stream()
                            .map(
                                    xref -> {
                                        if (Objects.equals(xref.getDatabase().getName(), "GO")) {
                                            return addGoEvidences(xref, goEvidenceMap);
                                        } else {
                                            return xref;
                                        }
                                    })
                            .collect(Collectors.toList());
            entry.uniProtCrossReferencesSet(xrefs);
        }
        return entry.build();
    }

    private UniProtkbCrossReference addGoEvidences(
            UniProtkbCrossReference xref, Map<String, List<Evidence>> goEvidenceMap) {
        String id = xref.getId();
        List<Evidence> evidences = goEvidenceMap.get(id);
        if ((evidences == null) || (evidences.isEmpty())) {
            return xref;
        } else {
            return new UniProtCrossReferenceBuilder()
                    .database(xref.getDatabase())
                    .id(xref.getId())
                    .isoformId(xref.getIsoformId())
                    .evidencesSet(evidences)
                    .propertiesSet(xref.getProperties())
                    .build();
        }
    }

    private Map<String, List<Evidence>> getGoEvidenceMap(Iterable<GOEvidence> goEvidences) {
        Map<String, List<Evidence>> goEvidenceMap = new HashMap<>();

        goEvidences.forEach(
                goEvidence -> {
                    List<Evidence> values =
                            goEvidenceMap.computeIfAbsent(
                                    goEvidence.getGoId(), goId -> new ArrayList<>());
                    values.add(goEvidence.getEvidence());
                });

        return goEvidenceMap;
    }
}
