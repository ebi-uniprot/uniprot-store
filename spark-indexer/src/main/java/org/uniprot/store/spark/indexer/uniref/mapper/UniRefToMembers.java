package org.uniprot.store.spark.indexer.uniref.mapper;

import static org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore.getVoldemortKey;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;

import scala.Tuple2;

/**
 * This class gets an UniRefEntry and extract the list of members and returns an iterator of list of
 * Tuple{key=voldemortKey, value=member}. The voldemortKey we use to join the members of 3 clusters
 * is either UniProt Accession Id or UniParc Id. See {@link
 * org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore#getVoldemortKey(UniRefMember)}
 * to find how we extract the voldemortKey from member
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

        RepresentativeMember representativeMember =
                RepresentativeMemberBuilder.from(entry.getRepresentativeMember())
                        .isSeed(null)
                        .build();

        // representative member
        results.add(new Tuple2<>(getVoldemortKey(representativeMember), representativeMember));

        // get other members
        entry.getMembers().stream()
                .map(this::convertToRepMember)
                .map(repMember -> new Tuple2<>(getVoldemortKey(repMember), repMember))
                .forEach(results::add);

        return results.iterator();
    }

    private RepresentativeMember convertToRepMember(UniRefMember member) {
        return RepresentativeMemberBuilder.from(member).isSeed(null).build();
    }
}
