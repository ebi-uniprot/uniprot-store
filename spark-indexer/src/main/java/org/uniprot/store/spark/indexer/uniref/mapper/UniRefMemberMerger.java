package org.uniprot.store.spark.indexer.uniref.mapper;

import java.util.Objects;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;
import org.uniprot.core.util.Utils;

import scala.Tuple2;

/**
 * This class merges the two members (tuple) into one. The member with sequence has precedence over
 * member without sequence.
 *
 * @author sahmad
 * @created 21/07/2020
 */
public class UniRefMemberMerger
        implements Function<
                Tuple2<RepresentativeMember, RepresentativeMember>, RepresentativeMember> {
    @Override
    public RepresentativeMember call(Tuple2<RepresentativeMember, RepresentativeMember> memberPair)
            throws Exception {

        if (Objects.isNull(memberPair._1)) {
            return memberPair._2;
        } else if (Objects.isNull(memberPair._2)) {
            return memberPair._1;
        } else if (Objects.nonNull(memberPair._1.getSequence())) {
            RepresentativeMember source = memberPair._2;
            RepresentativeMember target = memberPair._1;
            return mergeSourceToTarget(source, target);
        } else {
            RepresentativeMember source = memberPair._1;
            RepresentativeMember target = memberPair._2;
            return mergeSourceToTarget(source, target);
        }
    }

    // set value from source to target if target doesnt have already set
    private RepresentativeMember mergeSourceToTarget(
            RepresentativeMember source, RepresentativeMember target) {
        RepresentativeMemberBuilder builder = RepresentativeMemberBuilder.from(target);

        if (Objects.isNull(target.getMemberIdType()) && Objects.nonNull(source.getMemberIdType())) {
            builder.memberIdType(source.getMemberIdType());
        }

        if (Utils.nullOrEmpty(target.getMemberId())
                && Utils.notNullNotEmpty(source.getMemberId())) {
            builder.memberId(source.getMemberId());
        }

        if (Utils.nullOrEmpty(target.getOrganismName())
                && Utils.notNullNotEmpty(source.getOrganismName())) {
            builder.organismName(source.getOrganismName());
        }

        if (target.getOrganismTaxId() == 0l && source.getOrganismTaxId() != 0l) {
            builder.organismTaxId(source.getOrganismTaxId());
        }

        if (target.getSequenceLength() == 0 && source.getSequenceLength() != 0) {
            builder.sequenceLength(source.getSequenceLength());
        }

        if (Utils.nullOrEmpty(target.getProteinName())
                && Utils.notNullNotEmpty(source.getProteinName())) {
            builder.proteinName(source.getProteinName());
        }

        if (Utils.nullOrEmpty(target.getUniProtAccessions())
                && Utils.notNullNotEmpty(source.getUniProtAccessions())) {
            builder.accessionsSet(source.getUniProtAccessions());
        }
        if (Objects.isNull(target.getUniRef50Id()) && Objects.nonNull(source.getUniRef50Id())) {
            builder.uniref50Id(source.getUniRef50Id());
        }
        if (Objects.isNull(target.getUniRef90Id()) && Objects.nonNull(source.getUniRef90Id())) {
            builder.uniref90Id(source.getUniRef90Id());
        }

        if (Objects.isNull(target.getUniRef100Id()) && Objects.nonNull(source.getUniRef100Id())) {
            builder.uniref100Id(source.getUniRef100Id());
        }

        if (Objects.isNull(target.getUniParcId()) && Objects.nonNull(source.getUniParcId())) {
            builder.uniparcId(source.getUniParcId());
        }

        if (Objects.isNull(target.getOverlapRegion())
                && Objects.nonNull(source.getOverlapRegion())) {
            builder.overlapRegion(source.getOverlapRegion());
        }

        if (Objects.isNull(target.isSeed()) && Objects.nonNull(source.isSeed())) {
            builder.isSeed(source.isSeed());
        }

        return builder.build();
    }
}
