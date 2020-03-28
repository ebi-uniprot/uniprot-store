package org.uniprot.store.spark.indexer.go.evidence;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.core.uniprotkb.xdb.UniProtKBCrossReference;
import org.uniprot.core.uniprotkb.xdb.impl.UniProtCrossReferenceBuilder;

import scala.Tuple2;

/**
 * This class is responsible to map an Iterable of GOEvidence to an UniProtKBEntry
 *
 * @author lgonzales
 * @since 2019-10-22
 */
public class GOEvidenceMapper
        implements Function<
                Tuple2<UniProtKBEntry, Optional<Iterable<GOEvidence>>>, UniProtKBEntry> {

    private static final long serialVersionUID = 7478726902589041984L;

    /**
     * @param tuple of <UniProtKBEntry, Iterable of GoEvidence>
     * @return UniProtKBEntry with extra Go Evidences
     */
    @Override
    public UniProtKBEntry call(Tuple2<UniProtKBEntry, Optional<Iterable<GOEvidence>>> tuple)
            throws Exception {
        UniProtKBEntryBuilder entry = UniProtKBEntryBuilder.from(tuple._1);
        if (tuple._2.isPresent()) {
            Map<String, List<Evidence>> goEvidenceMap = getGoEvidenceMap(tuple._2.get());

            List<UniProtKBCrossReference> xrefs =
                    tuple._1.getUniProtKBCrossReferences().stream()
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

    private UniProtKBCrossReference addGoEvidences(
            UniProtKBCrossReference xref, Map<String, List<Evidence>> goEvidenceMap) {
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
