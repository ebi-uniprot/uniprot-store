package indexer.uniref;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.*;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.core.util.Utils;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
public class UniRefRDDTupleReader implements Serializable {
    private static final long serialVersionUID = -4292235298850285242L;

    private static Dataset<Row> readRawXml(SparkConf sparkConf, String xmlFilePath) {
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        Dataset<Row> data = spark.read().format("com.databricks.spark.xml")
                .option("rowTag", "entry")
                .schema(DatasetUnirefEntryConverter.getUniRefXMLSchema())
                .load(xmlFilePath);
        data.printSchema();
        return data;
    }

    public static JavaPairRDD<String, MappedUniRef> read50(SparkConf sparkConf, ResourceBundle applicationConfig) {
        String xmlFilePath = applicationConfig.getString("uniref.50.xml.file");
        Integer repartition = new Integer(applicationConfig.getString("uniref.50.repartition"));
        return getMappedUniRef(sparkConf, xmlFilePath, UniRefType.UniRef50, repartition);
    }

    public static JavaPairRDD<String, MappedUniRef> read90(SparkConf sparkConf, ResourceBundle applicationConfig) {
        String xmlFilePath = applicationConfig.getString("uniref.90.xml.file");
        Integer repartition = new Integer(applicationConfig.getString("uniref.90.repartition"));
        return getMappedUniRef(sparkConf, xmlFilePath, UniRefType.UniRef90, repartition);
    }

    public static JavaPairRDD<String, MappedUniRef> read100(SparkConf sparkConf, ResourceBundle applicationConfig) {
        String xmlFilePath = applicationConfig.getString("uniref.100.xml.file");
        Integer repartition = new Integer(applicationConfig.getString("uniref.100.repartition"));
        return getMappedUniRef(sparkConf, xmlFilePath, UniRefType.UniRef100, repartition);
    }

    private static JavaPairRDD<String, MappedUniRef> getMappedUniRef(SparkConf sparkConf, String xmlFilePath,
                                                                     UniRefType uniRefType, Integer repartition) {
        Dataset<Row> uniRefEntryDataset = readRawXml(sparkConf, xmlFilePath);
        if (repartition > 0) {
            uniRefEntryDataset = uniRefEntryDataset.repartition(repartition);
        }

        Encoder<UniRefEntry> entryEncoder = (Encoder<UniRefEntry>) Encoders.kryo(UniRefEntry.class);
        return (JavaPairRDD<String, MappedUniRef>) uniRefEntryDataset
                .map(new DatasetUnirefEntryConverter(uniRefType), entryEncoder)
                .toJavaRDD().flatMapToPair(new UniRefRDDTupleMapper());
    }


    private static class UniRefRDDTupleMapper implements PairFlatMapFunction<UniRefEntry, String, MappedUniRef>, Serializable {

        private static final long serialVersionUID = -6357861588356599290L;

        @Override
        public Iterator<Tuple2<String, MappedUniRef>> call(UniRefEntry uniRefEntry) throws Exception {
            List<Tuple2<String, MappedUniRef>> mappedAccessions = new ArrayList<>();

            List<String> memberAccessions = uniRefEntry.getMembers().stream()
                    .filter(uniRefMember -> uniRefMember.getMemberIdType() == UniRefMemberIdType.UNIPROTKB)
                    .map(uniRefMember -> uniRefMember.getUniProtAccession().getValue())
                    .collect(Collectors.toList());

            if (uniRefEntry.getRepresentativeMember().getMemberIdType() == UniRefMemberIdType.UNIPROTKB) {
                String accession = uniRefEntry.getRepresentativeMember().getUniProtAccession().getValue();
                memberAccessions.add(accession);
                MappedUniRef mappedUniRef = MappedUniRef.builder()
                        .uniRefType(uniRefEntry.getEntryType())
                        .clusterID(uniRefEntry.getId().getValue())
                        .uniRefMember(uniRefEntry.getRepresentativeMember())
                        .memberAccessions(memberAccessions)
                        .build();

                mappedAccessions.add(new Tuple2<>(accession, mappedUniRef));
            }

            if (Utils.notEmpty(uniRefEntry.getMembers())) {
                uniRefEntry.getMembers().forEach(uniRefMember -> {
                    if (uniRefMember.getMemberIdType() == UniRefMemberIdType.UNIPROTKB) {
                        String accession = uniRefMember.getUniProtAccession().getValue();
                        MappedUniRef mappedUniRef = MappedUniRef.builder()
                                .uniRefType(uniRefEntry.getEntryType())
                                .clusterID(uniRefEntry.getId().getValue())
                                .uniRefMember(uniRefMember)
                                .memberAccessions(memberAccessions)
                                .build();

                        mappedAccessions.add(new Tuple2<>(accession, mappedUniRef));
                    }
                });
            }

            return mappedAccessions.iterator();
        }
    }
}
