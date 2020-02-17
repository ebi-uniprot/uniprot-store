package org.uniprot.store.spark.indexer.go.evidence;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.builder.UniProtEntryBuilder;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.core.uniprot.xdb.UniProtDBCrossReference;
import org.uniprot.core.uniprot.xdb.builder.UniProtDBCrossReferenceBuilder;

import scala.Tuple2;

/**
 * This class is responsible to map an Iterable of GOEvidence to an UniProtEntry
 *
 * @author lgonzales
 * @since 2019-10-22
 */
public class GOEvidenceMapper
        implements Function<Tuple2<UniProtEntry, Optional<Iterable<GOEvidence>>>, UniProtEntry> {

    private static final long serialVersionUID = 7478726902589041984L;

    /**
     * @param tuple of <UniProtEntry, Iterable of GoEvidence>
     * @return UniProtEntry with extra Go Evidences
     */
    @Override
    public UniProtEntry call(Tuple2<UniProtEntry, Optional<Iterable<GOEvidence>>> tuple)
            throws Exception {
        UniProtEntryBuilder entry = UniProtEntryBuilder.from(tuple._1);
        if (tuple._2.isPresent()) {
            Map<String, List<Evidence>> goEvidenceMap = getGoEvidenceMap(tuple._2.get());

            List<UniProtDBCrossReference> xrefs =
                    tuple._1.getDatabaseCrossReferences().stream()
                            .map(
                                    xref -> {
                                        if (Objects.equals(
                                                xref.getDatabaseType().getName(), "GO")) {
                                            return addGoEvidences(xref, goEvidenceMap);
                                        } else {
                                            return xref;
                                        }
                                    })
                            .collect(Collectors.toList());
            entry.databaseCrossReferencesSet(xrefs);
        }
        return entry.build();
    }

    private UniProtDBCrossReference addGoEvidences(
            UniProtDBCrossReference xref, Map<String, List<Evidence>> goEvidenceMap) {
        String id = xref.getId();
        List<Evidence> evidences = goEvidenceMap.get(id);
        if ((evidences == null) || (evidences.isEmpty())) {
            return xref;
        } else {
            return new UniProtDBCrossReferenceBuilder()
                    .databaseType(xref.getDatabaseType())
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