package org.uniprot.store.spark.indexer.uniref;

import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.uniref.mapper.UniRefMemberMerger;
import org.uniprot.store.spark.indexer.uniref.mapper.UniRefToMembers;
import org.uniprot.store.spark.indexer.uniref.writer.UniRefMemberDataStoreWriter;

import com.typesafe.config.Config;

/**
 * This class stores the members including representative member from UniRef100, UniRef90 and
 * UniRef50 clusters. The key(voldemortKey) we use to join the members of 3 clusters is either
 * UniProt Accession Id or UniParc Id. See {@link
 * org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore#getVoldemortKey(UniRefMember)}
 * to find how we extract the key from member 1. We left join UniRef100 members including
 * representative member to UniRef90 members including representative member on either UniProt
 * Accession Id or UniParc Id of the member. 2. Then we merge the members from UniRef100 and
 * UniRef90 and get a resultant table, lets say MergedUniRef100AndUniRef90. After that we repeat the
 * above 2 steps for table MergedUniRef100AndUniRef90 and UniRef50
 *
 * @author sahmad
 * @since 2020-07-21
 */
@Slf4j
public class UniRefMembersDataStoreIndexer implements DataStoreIndexer {

    private final JobParameter jobParameter;

    public UniRefMembersDataStoreIndexer(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public void indexInDataStore() {
        try {
            Config config = jobParameter.getApplicationConfig();
            DataStoreParameter parameter = getDataStoreParameter(config);
            // load the uniref100
            UniRefRDDTupleReader uniref100Reader =
                    new UniRefRDDTupleReader(UniRefType.UniRef100, jobParameter, false);
            JavaRDD<UniRefEntry> uniRef100RDD = uniref100Reader.load();

            // load the uniref90
            UniRefRDDTupleReader uniref90Reader =
                    new UniRefRDDTupleReader(UniRefType.UniRef90, jobParameter, false);
            JavaRDD<UniRefEntry> uniRef90RDD = uniref90Reader.load();

            // load the uniref50
            UniRefRDDTupleReader uniref50Reader =
                    new UniRefRDDTupleReader(UniRefType.UniRef50, jobParameter, false);
            JavaRDD<UniRefEntry> uniRef50RDD = uniref50Reader.load();

            // flat the members of uniref100
            JavaPairRDD<String, RepresentativeMember> memberIdMember100RDD =
                    uniRef100RDD.flatMapToPair(new UniRefToMembers());

            // flat the members of uniref90
            JavaPairRDD<String, RepresentativeMember> memberIdMember90RDD =
                    uniRef90RDD.flatMapToPair(new UniRefToMembers());

            // flat the members of uniref50
            JavaPairRDD<String, RepresentativeMember> memberIdMember50RDD =
                    uniRef50RDD.flatMapToPair(new UniRefToMembers());
            // join the members on voldemortKey and merge
            JavaPairRDD<String, RepresentativeMember> memberIdMemberRDD =
                    memberIdMember100RDD
                            .leftOuterJoin(memberIdMember90RDD)
                            .mapValues(new UniRefMemberMerger())
                            .leftOuterJoin(memberIdMember50RDD)
                            .mapValues(new UniRefMemberMerger());

            // write to the store
            memberIdMemberRDD.values().foreachPartition(getWriter(parameter));
        } catch (Exception e) {
            throw new IndexDataStoreException(
                    "Execution error during uniref members data store index", e);
        } finally {
            log.info("Completed UniRef members data store index.");
        }
    }

    private DataStoreParameter getDataStoreParameter(Config config) {
        String numberOfConnections = config.getString("store.uniref.members.numberOfConnections");
        String maxRetry = config.getString("store.uniref.members.retry");
        String delay = config.getString("store.uniref.members.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.uniref.members.host"))
                .storeName(config.getString("store.uniref.members.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .brotliEnabled(config.getBoolean(BROTLI_COMPRESSION_ENABLED))
                .brotliLevel(config.getInt(BROTLI_COMPRESSION_LEVEL))
                .build();
    }

    VoidFunction<Iterator<RepresentativeMember>> getWriter(DataStoreParameter parameter) {
        return new UniRefMemberDataStoreWriter(parameter);
    }
}
