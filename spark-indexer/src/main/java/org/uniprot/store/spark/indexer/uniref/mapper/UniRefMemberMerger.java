package org.uniprot.store.spark.indexer.uniref.mapper;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.store.datastore.member.uniref.UniRefRepMemberPairMerger;

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
            return new UniRefRepMemberPairMerger().apply(source, target);
        }
    }
}
