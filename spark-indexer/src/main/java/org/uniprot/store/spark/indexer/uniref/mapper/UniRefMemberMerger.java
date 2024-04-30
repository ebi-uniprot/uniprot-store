package org.uniprot.store.spark.indexer.uniref.mapper;

import java.util.Objects;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;
import org.uniprot.core.util.Utils;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

/**
 * This class merges the two members (tuple) into one. The member with sequence has precedence over
 * member without sequence.
 *
 * @author sahmad
 * @created 21/07/2020
 */
@Slf4j
public class UniRefMemberMerger
        implements Function<
                Tuple2<RepresentativeMember, Optional<RepresentativeMember>>,
                RepresentativeMember> {
    @Override
    public RepresentativeMember call(
            Tuple2<RepresentativeMember, Optional<RepresentativeMember>> memberPair)
            throws Exception {
        if (!memberPair._2.isPresent()) {
            log.error(
                    "Member {} is not there in either UniRef90 and/or UniRef50",
                    memberPair._1.getMemberId());
            return memberPair._1;
        } else {
            RepresentativeMember source = memberPair._2.get();
            RepresentativeMember target = memberPair._1;
            return mergeMember(source, target);
        }
    }

    private RepresentativeMember mergeMember(
            RepresentativeMember source, RepresentativeMember target) {

        RepresentativeMemberBuilder builder = RepresentativeMemberBuilder.from(target);

        if (Objects.isNull(target.getMemberIdType())) {
            builder.memberIdType(source.getMemberIdType());
        }

        if (Utils.nullOrEmpty(target.getOrganismName())) {
            builder.organismName(source.getOrganismName());
        }

        if (target.getOrganismTaxId() == 0L) {
            builder.organismTaxId(source.getOrganismTaxId());
        }

        if (Objects.isNull(target.getSequence())) {
            builder.sequence(source.getSequence());
        }

        if (target.getSequenceLength() == 0) {
            builder.sequenceLength(source.getSequenceLength());
        }

        if (Utils.nullOrEmpty(target.getProteinName())) {
            builder.proteinName(source.getProteinName());
        }

        if (Utils.nullOrEmpty(target.getUniProtAccessions())) {
            builder.accessionsSet(source.getUniProtAccessions());
        }
        if (Objects.isNull(target.getUniRef50Id())) {
            builder.uniref50Id(source.getUniRef50Id());
        }
        if (Objects.isNull(target.getUniRef90Id())) {
            builder.uniref90Id(source.getUniRef90Id());
        }

        if (Objects.isNull(target.getUniRef100Id())) {
            builder.uniref100Id(source.getUniRef100Id());
        }

        if (Objects.isNull(target.getUniParcId())) {
            builder.uniparcId(source.getUniParcId());
        }

        if (Objects.isNull(target.getOverlapRegion())) {
            builder.overlapRegion(source.getOverlapRegion());
        }

        return builder.build();
    }
}
