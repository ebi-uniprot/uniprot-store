package org.uniprot.store.spark.indexer.uniref.mapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;

import scala.Tuple2;

/**
 * This class gets an UniRefEntry and extract the list of members and returns an iterator of list of
 * Tuple{key=memberId, value=member}.
 *
 * @author sahmad
 * @since 2020-07-21
 */
public class UniRefToMembers
        implements Serializable, PairFlatMapFunction<UniRefEntry, String, RepresentativeMember> {

    private static final long serialVersionUID = -8422527943779211567L;

    @Override
    public Iterator<Tuple2<String, RepresentativeMember>> call(UniRefEntry entry) throws Exception {
        List<Tuple2<String, RepresentativeMember>> results = new ArrayList<>();

        RepresentativeMember representativeMember = entry.getRepresentativeMember();
        // representative member
        results.add(new Tuple2<>(getMemberId(representativeMember), representativeMember));

        // get other members
        entry.getMembers().stream()
                .map(this::convertToRepMember)
                .map(repMember -> new Tuple2<>(getMemberId(repMember), repMember))
                .forEach(results::add);

        return results.iterator();
    }

    // get accession id if memberType is UniRefMemberIdType.UNIPROTKB
    private String getMemberId(UniRefMember member) {
        if (member.getMemberIdType() == UniRefMemberIdType.UNIPARC) {
            return member.getMemberId();
        } else {
            return member.getUniProtAccessions().get(0).getValue();
        }
    }

    private RepresentativeMember convertToRepMember(UniRefMember member) {
        return RepresentativeMemberBuilder.from(member).build();
    }
}
