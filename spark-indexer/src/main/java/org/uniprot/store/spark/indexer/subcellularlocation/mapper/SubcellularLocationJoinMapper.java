package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.core.uniprotkb.comment.SubcellularLocation;
import org.uniprot.core.uniprotkb.comment.SubcellularLocationComment;
import org.uniprot.core.uniprotkb.comment.SubcellularLocationValue;

import scala.Tuple2;

/**
 * Extracts the subcellular locations from a UniProt entry
 *
 * @author sahmad
 * @created 31/01/2022
 */
public class SubcellularLocationJoinMapper
        implements PairFlatMapFunction<
                Tuple2<String, UniProtKBEntry>, String, MappedProteinAccession> {
    // returns Iterator of Tuple2<SL-xxxx, MappedProteinAccession<accession, reviewed>>
    @Override
    public Iterator<Tuple2<String, MappedProteinAccession>> call(
            Tuple2<String, UniProtKBEntry> accessionEntry) throws Exception {
        UniProtKBEntry entry = accessionEntry._2;
        List<SubcellularLocationComment> comments =
                entry.getCommentsByType(CommentType.SUBCELLULAR_LOCATION);
        MappedProteinAccession mappedProteinAccession = getMappedProteinAccession(entry);
        List<String> locationSubcellIds =
                comments.stream()
                        .flatMap(comment -> comment.getSubcellularLocations().stream())
                        .filter(SubcellularLocation::hasLocation)
                        .map(SubcellularLocation::getLocation)
                        .map(SubcellularLocationValue::getId)
                        .collect(Collectors.toList());
        List<String> topologySubcellIds =
                comments.stream()
                        .flatMap(comment -> comment.getSubcellularLocations().stream())
                        .filter(SubcellularLocation::hasTopology)
                        .map(SubcellularLocation::getTopology)
                        .map(SubcellularLocationValue::getId)
                        .collect(Collectors.toList());
        List<String> orientationSubcellIds =
                comments.stream()
                        .flatMap(comment -> comment.getSubcellularLocations().stream())
                        .filter(SubcellularLocation::hasOrientation)
                        .map(SubcellularLocation::getOrientation)
                        .map(SubcellularLocationValue::getId)
                        .collect(Collectors.toList());
        locationSubcellIds.addAll(topologySubcellIds);
        locationSubcellIds.addAll(orientationSubcellIds);
        return locationSubcellIds.stream()
                .map(subcellId -> new Tuple2<>(subcellId, mappedProteinAccession))
                .iterator();
    }

    private MappedProteinAccession getMappedProteinAccession(UniProtKBEntry entry) {
        MappedProteinAccession.MappedProteinAccessionBuilder builder =
                new MappedProteinAccession.MappedProteinAccessionBuilder();
        if (!isIsoform(entry.getPrimaryAccession().getValue())) {
            boolean isReviewed = entry.getEntryType() == UniProtKBEntryType.SWISSPROT;
            builder.proteinAccession(entry.getPrimaryAccession().getValue());
            builder.isReviewed(isReviewed);
        }
        return builder.build();
    }

    private boolean isIsoform(String accession) {
        return accession.contains("-");
    }
}
