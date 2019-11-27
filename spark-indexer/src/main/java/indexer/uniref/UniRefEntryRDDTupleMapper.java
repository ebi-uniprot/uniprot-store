package indexer.uniref;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.util.Utils;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-11-22
 */
public class UniRefEntryRDDTupleMapper implements PairFlatMapFunction<UniRefEntry, String, MappedUniRef>, Serializable {

    private static final long serialVersionUID = -6357861588356599290L;

    @Override
    public Iterator<Tuple2<String, MappedUniRef>> call(UniRefEntry uniRefEntry) throws Exception {
        List<Tuple2<String, MappedUniRef>> mappedAccessions = new ArrayList<>();

        List<String> memberAccessions = uniRefEntry.getMembers().stream()
                .filter(uniRefMember -> uniRefMember.getMemberIdType() == UniRefMemberIdType.UNIPROTKB)
                .map(uniRefMember -> uniRefMember.getUniProtAccession().getValue())
                .collect(Collectors.toList());

        if (uniRefEntry.getRepresentativeMember().getMemberIdType() == UniRefMemberIdType.UNIPROTKB) {
            String accession = uniRefEntry.getRepresentativeMember().getUniProtAccession().getValue();
            memberAccessions.add(accession);
            MappedUniRef mappedUniRef = MappedUniRef.builder()
                    .uniRefType(uniRefEntry.getEntryType())
                    .clusterID(uniRefEntry.getId().getValue())
                    .uniRefMember(uniRefEntry.getRepresentativeMember())
                    .memberAccessions(memberAccessions)
                    .memberSize(uniRefEntry.getMemberCount())
                    .build();

            mappedAccessions.add(new Tuple2<>(accession, mappedUniRef));
        }

        if (Utils.notNullOrEmpty(uniRefEntry.getMembers())) {
            uniRefEntry.getMembers().forEach(uniRefMember -> {
                if (uniRefMember.getMemberIdType() == UniRefMemberIdType.UNIPROTKB) {
                    String accession = uniRefMember.getUniProtAccession().getValue();
                    MappedUniRef mappedUniRef = MappedUniRef.builder()
                            .uniRefType(uniRefEntry.getEntryType())
                            .clusterID(uniRefEntry.getId().getValue())
                            .uniRefMember(uniRefMember)
                            .memberAccessions(memberAccessions)
                            .memberSize(uniRefEntry.getMemberCount())
                            .build();

                    mappedAccessions.add(new Tuple2<>(accession, mappedUniRef));
                }
            });
        }

        return mappedAccessions.iterator();
    }
}