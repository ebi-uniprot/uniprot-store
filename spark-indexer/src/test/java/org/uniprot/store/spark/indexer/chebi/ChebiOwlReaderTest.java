package org.uniprot.store.spark.indexer.chebi;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.spark.sql.*;

// @TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ChebiOwlReaderTest {

    //    StructType schema = new StructType()
    //            .add("_rdf:about", DataTypes.StringType, true)
    //            .add("_rdf:nodeID", DataTypes.StringType, true)
    //            .add("rdf:type", new StructType().add("_rdf:resource", DataTypes.StringType,
    // true), true)
    //            .add("chebiStructuredName", DataTypes.createArrayType(new
    // StructType().add("_rdf:nodeID", DataTypes.StringType, true)), true)
    //            .add("chebislash:inchikey", DataTypes.StringType, true)
    //            .add("obo:IAO_0000115", DataTypes.StringType, true)
    //            .add("oboInOwl:hasId", DataTypes.StringType, true)
    //            .add("rdfs:subClassOf", DataTypes.createArrayType(new
    // StructType().add("_rdf:resource", DataTypes.StringType, true).add("_rdf:nodeID",
    // DataTypes.StringType, true)), true)
    //            .add("rdfs:label", DataTypes.createArrayType(DataTypes.StringType), true)
    //            .add("owl:onProperty", DataTypes.createArrayType(new
    // StructType().add("_rdf:resource", DataTypes.StringType, true)), true)
    //            .add("owl:someValuesFrom", DataTypes.createArrayType(new
    // StructType().add("_rdf:resource", DataTypes.StringType, true)), true);
    //
    //    private SparkSession spark;
    //
    //    @BeforeAll
    //    public void setUp() {
    //        spark = SparkSession.builder()
    //                .appName("ChebiOwlReaderTest")
    //                .master("local[*]")
    //                .getOrCreate();
    //    }
    //
    //    @Test
    //    public void testGetLabelAndClassColumnsFromAboutRDD() {
    //        // Mock input data
    //        Dataset<Row> processedAboutDF = mock(Dataset.class);
    //        // Create mock data for processedAboutDF
    //        List<Row> aboutRows = Arrays.asList(
    //                RowFactory.create("CHEBI_77793", new
    // scala.collection.mutable.HashMap<>().$plus$eq(Tuple2.apply("chebiStructuredName",
    // Arrays.asList("name209019", "name209020", "name209021")))),
    //                RowFactory.create("CHEBI_77793", new
    // scala.collection.mutable.HashMap<>().$plus$eq(Tuple2.apply("rdfs:subClassOf",
    // Arrays.asList("CHEBI_25697", "CHEBI_35274"))))
    //        );
    //        when(processedAboutDF.collectAsList()).thenReturn(aboutRows);
    //        // Call the getLabelAndClassColumnsFromAboutRDD method with mock data
    //        StructType explodedSchema = new StructType()
    //                .add("about_subject", DataTypes.StringType, false)
    //                .add("chebiStructuredName", DataTypes.StringType, true)
    //                .add("rdfs:subClassOf", DataTypes.StringType, true);
    //        Dataset<Row> result =
    // ChebiOwlReader.getLabelAndClassColumnsFromAboutRDD(explodedSchema, processedAboutDF);
    //        // Verify the result
    //        List<Row> resultRows = result.collectAsList();
    //        assert resultRows.size() == 5;
    //        assert resultRows.get(0).getString(0).equals("CHEBI_77793");
    //        assert resultRows.get(0).getAs(1).equals("name209019");
    //        assert resultRows.get(1).getString(0).equals("CHEBI_77793");
    //        assert resultRows.get(1).getAs(1).equals("name209020");
    //        assert resultRows.get(2).getString(0).equals("CHEBI_77793");
    //        assert resultRows.get(2).getAs(1).equals("name209021");
    //        assert resultRows.get(3).getString(0).equals("CHEBI_77793");
    //        assert resultRows.get(3).getAs(2).equals("CHEBI_25697");
    //        assert resultRows.get(4).getString(0).equals("CHEBI_77793");
    //        assert resultRows.get(4).getAs(2).equals("CHEBI_35274");
    //    }
    //
    //    @Test
    //    public void testGetLabelColumnsFromAboutRDD_withNullSchema() {
    //        // Mock input data
    //        Dataset<Row> processedAboutDF = mock(Dataset.class);
    //        // Call the getLabelColumnsFromAboutRDD method with a null schema
    //        Dataset<Row> result = ChebiOwlReader.getLabelAndClassColumnsFromAboutRDD(null,
    // processedAboutDF);
    //        // Verify the result is null
    //        assertNull(result);
    //    }
    //
    //    @Test
    //    public void testGetLabelColumnsFromAboutRDD_withNullProcessedAboutDF() {
    //        // Call the getLabelColumnsFromAboutRDD method with a null DataFrame
    //        Dataset<Row> result = ChebiOwlReader.getLabelAndClassColumnsFromAboutRDD(new
    // StructType(), null);
    //        // Verify the result is null
    //        assertNull(result);
    //    }
    //
    //    @Test
    //    public void testGetLabelColumnsFromAboutRDD_withEmptyProcessedAboutDF() {
    //        // Mock input data
    //        Dataset<Row> processedAboutDF = mock(Dataset.class);
    //        when(processedAboutDF.collectAsList()).thenReturn(Collections.emptyList());
    //        // Call the getLabelColumnsFromAboutRDD method with an empty DataFrame
    //        Dataset<Row> result = ChebiOwlReader.getLabelAndClassColumnsFromAboutRDD(new
    // StructType(), processedAboutDF);
    //        // Verify the result is an empty DataFrame
    //        assertTrue(result.isEmpty());
    //    }

    //    @Test
    //    public void testJoinAndExtractLabelAndClassRelatedNodesFromNodeIdDF() {
    //        // Mock input data
    //        Dataset<Row> nodeIdDF = mock(Dataset.class);
    //        Dataset<Row> aboutDF = mock(Dataset.class);
    //
    //        // Create mock data for nodeIdDF
    //        StructType nodeIdSchema = new StructType()
    //                .add("subject", DataTypes.StringType, false)
    //                .add("object", DataTypes.StringType, false)
    //                .add("predicate", DataTypes.StringType, false);
    //        List<Row> nodeIdRows = Arrays.asList(
    //                RowFactory.create("CHEBI_77793", "CHEBI_25697", "rdfs:subClassOf"),
    //                RowFactory.create("CHEBI_77793", "CHEBI_35274", "rdfs:subClassOf")
    //        );
    //        when(nodeIdDF.schema()).thenReturn(nodeIdSchema);
    //        when(nodeIdDF.collectAsList()).thenReturn(nodeIdRows);
    //
    //        // Create mock data for aboutDF
    //        StructType aboutSchema = new StructType()
    //                .add("about_subject", DataTypes.StringType, false)
    //                .add("chebiStructuredName", DataTypes.StringType, true)
    //                .add("rdfs:subClassOf", DataTypes.StringType, true);
    //        List<Row> aboutRows = Arrays.asList(
    //                RowFactory.create("CHEBI_77793", "name209019", null),
    //                RowFactory.create("CHEBI_77793", "name209020", null),
    //                RowFactory.create("CHEBI_77793", "name209021", null),
    //                RowFactory.create("CHEBI_25697", null, null),
    //                RowFactory.create("CHEBI_35274", null, null)
    //        );
    //        when(aboutDF.schema()).thenReturn(aboutSchema);
    //        when(aboutDF.collectAsList()).thenReturn(aboutRows);
    //
    //        // Call the joinAndExtractLabelAndClassRelatedNodesFromNodeIdDF method with mock data
    //        JavaRDD<Row> result =
    // ChebiOwlReader.joinAndExtractLabelAndClassRelatedNodesFromNodeIdDF(nodeIdDF, aboutDF);
    //
    //        // Verify the result
    //        List<Row> resultRows = result.collectAsList();
    //        assert resultRows.size() == 3;
    //        assert resultRows.get(0).getString(0).equals("CHEBI_77793");
    //        assert resultRows.get(0).getAs(1).equals("name209019");
    //        assert resultRows.get(0).getAs(2).equals("CHEBI_25697");
    //        assert resultRows.get(1).getString(0).equals("CHEBI_77793");
    //        assert resultRows.get(1).getAs(1).equals("name209019");
    //        assert resultRows.get(1).getAs(2).equals("CHEBI_35274");
    //        assert resultRows.get(2).getString(0).equals("CHEBI_77793");
    //        assert resultRows.get(2).getAs(1).equals("name209020");
    //        assert resultRows.get(2).getAs(2).equals("CHEBI_35274");
    //    }
    //
    //
    //    @Test
    //    public void testGetNodeIdJavaRDDFromDescription() {
    //        // Mock input data
    //        Dataset<Row> descriptionDF = mock(Dataset.class);
    //        // Create mock data for descriptionDF
    //        List<Row> descriptionRows = Arrays.asList(
    //                RowFactory.create("http://purl.obolibrary.org/obo/CHEBI_77793", "glycoside"),
    //                RowFactory.create("http://purl.obolibrary.org/obo/CHEBI_25697", "organic
    // molecular entity"),
    //                RowFactory.create("http://purl.obolibrary.org/obo/CHEBI_35274", "polyketide")
    //        );
    //        when(descriptionDF.collectAsList()).thenReturn(descriptionRows);
    //        // Call the getNodeIdJavaRDDFromDescription method with mock data
    //        JavaRDD<String> result =
    // ChebiOwlReader.getNodeIdJavaRDDFromDescription(descriptionDF);
    //        // Verify the result
    //        List<String> resultList = result.collect();
    //        assertEquals(resultList.size(), 3);
    //        assertEquals(resultList.get(0), "CHEBI_77793");
    //        assertEquals(resultList.get(1), "CHEBI_25697");
    //        assertEquals(resultList.get(2), "CHEBI_35274");
    //    }
    //
    //    @Test
    //    public void testGetAboutJavaRDDFromDescription() {
    //        // Mock input data
    //        Dataset<Row> descriptionDF = mock(Dataset.class);
    //        // Create mock data for descriptionDF
    //        List<Row> descriptionRows = Arrays.asList(
    //                RowFactory.create("http://purl.obolibrary.org/obo/CHEBI_77793", "glycoside"),
    //                RowFactory.create("http://purl.obolibrary.org/obo/CHEBI_25697", "organic
    // molecular entity"),
    //                RowFactory.create("http://purl.obolibrary.org/obo/CHEBI_35274", "polyketide")
    //        );
    //        when(descriptionDF.collectAsList()).thenReturn(descriptionRows);
    //        // Call the getAboutJavaRDDFromDescription method with mock data
    //        JavaRDD<String> result = ChebiOwlReader.getAboutJavaRDDFromDescription(descriptionDF);
    //        // Verify the result
    //        List<String> resultList = result.collect();
    //        assertEquals(resultList.size(), 3);
    //        assertEquals(resultList.get(0), "<http://purl.obolibrary.org/obo/CHEBI_77793>");
    //        assertEquals(resultList.get(1), "<http://purl.obolibrary.org/obo/CHEBI_25697>");
    //        assertEquals(resultList.get(2), "<http://purl.obolibrary.org/obo/CHEBI_35274>");
    //    }
}
