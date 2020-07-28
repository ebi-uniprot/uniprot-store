package org.uniprot.store.datastore.member.uniref;

import java.util.Objects;
import java.util.function.BiFunction;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;
import org.uniprot.core.util.Utils;

/**
 * @@author sahmad
 *
 * @created 28/07/2020
 */
public class UniRefRepMemberPairMerger
        implements BiFunction<RepresentativeMember, RepresentativeMember, RepresentativeMember> {
    @Override
    public RepresentativeMember apply(RepresentativeMember source, RepresentativeMember target) {
        RepresentativeMemberBuilder builder = RepresentativeMemberBuilder.from(target);

        if (Objects.isNull(target.getMemberIdType()) && Objects.nonNull(source.getMemberIdType())) {
            builder.memberIdType(source.getMemberIdType());
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

        return builder.build();
    }
}
