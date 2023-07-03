package org.uniprot.store.spark.indexer.uniref;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.go.GoAspect;
import org.uniprot.core.cv.go.impl.GeneOntologyEntryBuilder;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 14/07/2020
 */
public class UniRefXmlUtils {

    public static final String PROPERTY_MEMBER_COUNT = "member count";
    public static final String PROPERTY_COMMON_TAXON = "common taxon";
    public static final String PROPERTY_COMMON_TAXON_ID = "common taxon ID";
    public static final String PROPERTY_GO_FUNCTION = "GO Molecular Function";
    public static final String PROPERTY_GO_COMPONENT = "GO Cellular Component";
    public static final String PROPERTY_GO_PROCESS = "GO Biological Process";

    public static final String PROPERTY_ACCESSION = "UniProtKB accession";
    public static final String PROPERTY_UNIPARC_ID = "UniParc ID";
    public static final String PROPERTY_UNIREF_50_ID = "UniRef50 ID";
    public static final String PROPERTY_UNIREF_90_ID = "UniRef90 ID";
    public static final String PROPERTY_UNIREF_100_ID = "UniRef100 ID";
    public static final String PROPERTY_OVERLAP_REGION = "overlap region";
    public static final String PROPERTY_PROTEIN_NAME = "protein name";
    public static final String PROPERTY_ORGANISM = "source organism";
    public static final String PROPERTY_TAXONOMY = "NCBI taxonomy";
    public static final String PROPERTY_LENGTH = "length";
    public static final String PROPERTY_IS_SEED = "isSeed";

    public static final String DB_REFERENCE = "dbReference";
    public static final String SEQUENCE = "sequence";
    public static final String ID = "_id";
    public static final String NAME = "name";
    public static final String UPDATED = "_updated";
    public static final String MEMBER = "member";
    public static final String PROPERTY = "property";
    public static final String REPRESENTATIVE_MEMBER = "representativeMember";

    private UniRefXmlUtils() {}

    static Dataset<Row> loadRawXml(UniRefType uniRefType, JobParameter jobParameter) {
        Config config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String propertyPrefix = uniRefType.toString().toLowerCase();
        String xmlFilePath = releaseInputDir + config.getString(propertyPrefix + ".xml.file");

        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        Dataset<Row> data =
                spark.read()
                        .format("com.databricks.spark.xml")
                        .option("rowTag", "entry")
                        .schema(getUniRefXMLSchema())
                        .load(xmlFilePath);
        data.printSchema();
        return data;
    }

    public static StructType getUniRefXMLSchema() {
        StructType structType = new StructType();
        structType = structType.add(ID, DataTypes.StringType, true);
        structType = structType.add(UPDATED, DataTypes.StringType, true);
        structType = structType.add(MEMBER, DataTypes.createArrayType(getMemberSchema()), true);
        structType = structType.add(NAME, DataTypes.StringType, true);
        structType =
                structType.add(
                        PROPERTY, DataTypes.createArrayType(RowUtils.getPropertySchema()), true);
        structType = structType.add(REPRESENTATIVE_MEMBER, getRepresentativeMemberSchema(), true);
        return structType;
    }

    public static StructType getRepresentativeMemberSchema() {
        StructType representativeMember = getMemberSchema();
        representativeMember =
                representativeMember.add(SEQUENCE, RowUtils.getSequenceSchema(), true);
        return representativeMember;
    }

    public static StructType getMemberSchema() {
        StructType member = new StructType();
        member = member.add(DB_REFERENCE, RowUtils.getDBReferenceSchema(), true);
        return member;
    }

    public static List<GeneOntologyEntry> convertUniRefGoTermsProperties(
            Map<String, List<String>> propertyMap) {
        List<GeneOntologyEntry> goTerms = new ArrayList<>();
        if (propertyMap.containsKey(PROPERTY_GO_FUNCTION)) {
            propertyMap.get(PROPERTY_GO_FUNCTION).stream()
                    .map(goTerm -> createGoTerm(GoAspect.FUNCTION, goTerm))
                    .forEach(goTerms::add);
        }
        if (propertyMap.containsKey(PROPERTY_GO_COMPONENT)) {
            propertyMap.get(PROPERTY_GO_COMPONENT).stream()
                    .map(goTerm -> createGoTerm(GoAspect.COMPONENT, goTerm))
                    .forEach(goTerms::add);
        }
        if (propertyMap.containsKey(PROPERTY_GO_PROCESS)) {
            propertyMap.get(PROPERTY_GO_PROCESS).stream()
                    .map(goTerm -> createGoTerm(GoAspect.PROCESS, goTerm))
                    .forEach(goTerms::add);
        }
        return goTerms;
    }

    public static GeneOntologyEntry createGoTerm(GoAspect type, String id) {
        return new GeneOntologyEntryBuilder().aspect(type).id(id).build();
    }
}
