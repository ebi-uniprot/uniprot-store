package org.uniprot.store.datastore.member.uniref;

import java.util.Objects;
import java.util.function.BinaryOperator;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;
import org.uniprot.core.util.Utils;

/**
 * @@author sahmad
 *
 * @created 28/07/2020
 */
public class UniRefRepMemberPairMerger implements BinaryOperator<RepresentativeMember> {

    // set value from source to target if target doesnt have already set
    @Override
    public RepresentativeMember apply(RepresentativeMember source, RepresentativeMember target) {

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
