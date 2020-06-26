package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniprotkb.UniProtKBAccession;
import org.uniprot.core.uniref.*;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.uniprot.mapper.model.MappedUniRef;

import scala.Serializable;
import scala.Tuple2;

/**
 * This class is Responsible to Map a UniRefEntry, to a Tuple{key=accession, value=MappedUniRef}
 *
 * @author lgonzales
 * @since 2019-11-22
 */
public class UniRefJoinMapper
        implements PairFlatMapFunction<UniRefEntry, String, MappedUniRef>, Serializable {

    private static final long serialVersionUID = -6357861588356599290L;

    /**
     * @param uniRefEntry UniRefEntry
     * @return an Iterator of Tuple{key=accession, value=MappedUniRef} for each UNIPROTKB
     *     member/representativeMember
     */
    @Override
    public Iterator<Tuple2<String, MappedUniRef>> call(UniRefEntry uniRefEntry) throws Exception {
        List<Tuple2<String, MappedUniRef>> mappedAccessions = new ArrayList<>();
        UniRefType type = uniRefEntry.getEntryType();
        String clusterId = uniRefEntry.getId().getValue();
        if (uniRefEntry.getRepresentativeMember().getMemberIdType()
                == UniRefMemberIdType.UNIPROTKB) {
            mappedAccessions.addAll(
                    getMappedUnirefsForMember(
                            type, clusterId, uniRefEntry.getRepresentativeMember()));
        }

        if (Utils.notNullNotEmpty(uniRefEntry.getMembers())) {
            uniRefEntry
                    .getMembers()
                    .forEach(
                            uniRefMember -> {
                                if (uniRefMember.getMemberIdType()
                                        == UniRefMemberIdType.UNIPROTKB) {
                                    mappedAccessions.addAll(
                                            getMappedUnirefsForMember(
                                                    type, clusterId, uniRefMember));
                                }
                            });
        }

        return mappedAccessions.iterator();
    }

    private List<Tuple2<String, MappedUniRef>> getMappedUnirefsForMember(
            UniRefType type, String clusterId, UniRefMember uniRefMember) {
        List<String> accessions =
                uniRefMember.getUniProtAccessions().stream()
                        .map(UniProtKBAccession::getValue)
                        .collect(Collectors.toList());
        MappedUniRef mappedUniRef =
                MappedUniRef.builder()
                        .uniRefType(type)
                        .clusterID(clusterId)
                        .uniparcUPI(getUniparcUPIFromMember(uniRefMember))
                        .build();

        return accessions.stream()
                .map(accession -> new Tuple2<>(accession, mappedUniRef))
                .collect(Collectors.toList());
    }

    private String getUniparcUPIFromMember(UniRefMember member) {
        String result = "";
        if (Utils.notNull(member.getUniParcId())) {
            result = member.getUniParcId().getValue();
        }
        return result;
    }
}
