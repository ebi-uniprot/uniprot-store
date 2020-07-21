package org.uniprot.store.spark.indexer.uniref;

import java.util.Iterator;
import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.uniref.mapper.UniRefMemberMerger;
import org.uniprot.store.spark.indexer.uniref.mapper.UniRefToMembers;
import org.uniprot.store.spark.indexer.uniref.writer.UniRefMemberDataStoreWriter;

/**
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
        ResourceBundle config = jobParameter.getApplicationConfig();
        final String connectionURL = config.getString("store.uniref.members.host");
        final String numberOfConnections =
                config.getString("store.uniref.members.numberOfConnections");
        final String storeName = config.getString("store.uniref.members.storeName");
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
                new UniRefRDDTupleReader(UniRefType.UniRef90, jobParameter, false);
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
        // join the members and merge
        JavaPairRDD<String, RepresentativeMember> memberIdMemberRDD =
                memberIdMember100RDD
                        .join(memberIdMember90RDD)
                        .mapValues(new UniRefMemberMerger())
                        .join(memberIdMember50RDD)
                        .mapValues(new UniRefMemberMerger());

        memberIdMemberRDD
                .values()
                .foreachPartition(getWriter(numberOfConnections, storeName, connectionURL));
    }

    private VoidFunction<Iterator<RepresentativeMember>> getWriter(
            String numberOfConnections, String storeName, String connectionURL) {
        return new UniRefMemberDataStoreWriter(numberOfConnections, storeName, connectionURL);
    }
}
