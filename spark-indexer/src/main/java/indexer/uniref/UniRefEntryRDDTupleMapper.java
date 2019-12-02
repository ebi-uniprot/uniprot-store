package indexer.uniref;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniprot.UniProtAccession;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.util.Utils;

import scala.Serializable;
import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-22
 */
public class UniRefEntryRDDTupleMapper
        implements PairFlatMapFunction<UniRefEntry, String, MappedUniRef>, Serializable {

    private static final long serialVersionUID = -6357861588356599290L;

    @Override
    public Iterator<Tuple2<String, MappedUniRef>> call(UniRefEntry uniRefEntry) throws Exception {
        List<Tuple2<String, MappedUniRef>> mappedAccessions = new ArrayList<>();

        List<String> memberAccessions =
                uniRefEntry.getMembers().stream()
                        .filter(
                                uniRefMember ->
                                        uniRefMember.getMemberIdType()
                                                == UniRefMemberIdType.UNIPROTKB)
                        .flatMap(
                                uniRefMember ->
                                        uniRefMember.getUniProtAccessions().stream()
                                                .map(UniProtAccession::getValue))
                        .collect(Collectors.toList());

        if (uniRefEntry.getRepresentativeMember().getMemberIdType()
                == UniRefMemberIdType.UNIPROTKB) {
            List<String> accessions =
                    uniRefEntry.getRepresentativeMember().getUniProtAccessions().stream()
                            .map(UniProtAccession::getValue)
                            .collect(Collectors.toList());
            memberAccessions.addAll(accessions);
            MappedUniRef mappedUniRef =
                    MappedUniRef.builder()
                            .uniRefType(uniRefEntry.getEntryType())
                            .clusterID(uniRefEntry.getId().getValue())
                            .uniRefMember(uniRefEntry.getRepresentativeMember())
                            .memberAccessions(memberAccessions)
                            .memberSize(uniRefEntry.getMemberCount())
                            .build();
            accessions.forEach(
                    accession -> {
                        mappedAccessions.add(new Tuple2<>(accession, mappedUniRef));
                    });
        }

        if (Utils.notNullOrEmpty(uniRefEntry.getMembers())) {
            uniRefEntry
                    .getMembers()
                    .forEach(
                            uniRefMember -> {
                                if (uniRefMember.getMemberIdType()
                                        == UniRefMemberIdType.UNIPROTKB) {
                                    List<String> accessions =
                                            uniRefMember.getUniProtAccessions().stream()
                                                    .map(UniProtAccession::getValue)
                                                    .collect(Collectors.toList());
                                    MappedUniRef mappedUniRef =
                                            MappedUniRef.builder()
                                                    .uniRefType(uniRefEntry.getEntryType())
                                                    .clusterID(uniRefEntry.getId().getValue())
                                                    .uniRefMember(uniRefMember)
                                                    .memberAccessions(memberAccessions)
                                                    .memberSize(uniRefEntry.getMemberCount())
                                                    .build();

                                    accessions.forEach(
                                            accession -> {
                                                mappedAccessions.add(
                                                        new Tuple2<>(accession, mappedUniRef));
                                            });
                                }
                            });
        }

        return mappedAccessions.iterator();
    }
}
