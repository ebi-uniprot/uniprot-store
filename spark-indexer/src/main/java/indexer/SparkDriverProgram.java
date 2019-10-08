package indexer;

import indexer.uniprot.DatasetUniprotRowConverter;
import indexer.uniprot.UniProtFlatFileReader;
import indexer.uniref.DatasetUnirefRowConverter;
import indexer.uniref.UniRefXmlReader;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniref.UniRefEntry;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.collect_list;

/**
 * @author jluo
 * @date: 2 Jul 2019
 */

public class SparkDriverProgram {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("UniProt-Spark-indexer")
                .setMaster("local[*]");
        //.setMaster("spark://lgonzales-ml:7077")
        //.set("spark.ui.port", "8080");
        System.out.println("******************* STARTING PROCCESS *************************");
        System.out.println("**************************************************************");
        JavaSparkContext sc = new JavaSparkContext(conf);


        //String flatFilePath = "/Users/lgonzales/Downloads/uniprot-files/uniprot_sprot.dat";
        //String humanFlatFilePath = "/Users/lgonzales/Downloads/uniprot-files/uniprot_human.dat";
        String flatFileP21802Path = "/Users/lgonzales/Downloads/uniprot-files/flatFilesP21802.dat";
        UniProtFlatFileReader flatFileReader = new UniProtFlatFileReader(conf, flatFileP21802Path);
        Dataset<UniProtEntry> uniProtEntryDataset = flatFileReader.read();

        ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(DatasetUniprotRowConverter.getRowSchema());
        Dataset<Row> uniprotRowDataset = uniProtEntryDataset.map(new DatasetUniprotRowConverter(), rowEncoder);




        /*System.out.println("------- explain ------------");
        uniprotRowDataset.explain();
        System.out.println("------- schema ------------");
        uniprotRowDataset.printSchema();
        System.out.println("------- items count ------------");
        System.out.println("Dataset<Row>: "+uniprotRowDataset.count());*/


        //String xmlSmallFile = "/Users/lgonzales/Downloads/uniprot-files/uniref50_small_sample.xml";
        //String xmlFile = "/Users/lgonzales/Downloads/uniprot-files/uniref50_sample.xml";
        String xmlP21802 = "/Users/lgonzales/Downloads/uniprot-files/UniRef_P21802.xml";
        UniRefXmlReader xmlReader = new UniRefXmlReader(conf, xmlP21802);
        Dataset<UniRefEntry> uniRefEntryDataset = xmlReader.read();

        ExpressionEncoder<Row> uniRefRowEncoder = RowEncoder.apply(DatasetUnirefRowConverter.getRowSchema());
        Dataset<Row> unirefRowDataset = uniRefEntryDataset.map(new DatasetUnirefRowConverter(), uniRefRowEncoder);
/*        Row[] headRows = (Row[]) unirefRowDataset.head(100);
        for(Row row : headRows){
            System.out.println("########### Cluster Id: "+row.getString(row.fieldIndex("clusterId")));
            List<String> accessionIdList= row.getList(row.fieldIndex("accessionIds"));
            System.out.println("---------------- Accession Ids: ");
            accessionIdList.forEach(System.out::println);

            List<String> upIdList= row.getList(row.fieldIndex("upIds"));
            System.out.println("--------------- UP Ids: ");
            upIdList.forEach(System.out::println);
        }*/

/*        System.out.println("------- explain ------------");
        uniRefEntryDataset.explain();
        System.out.println("------- schema ------------");
        uniRefEntryDataset.printSchema();
        System.out.println("------- show items id ------------");
        System.out.println("Dataset<UniRefEntry>: "+uniRefEntryDataset.count());*/


        Column accession = uniprotRowDataset.col("accession");
        Column uniprotEntry = uniprotRowDataset.col("uniprotEntry");

        Column cluster = unirefRowDataset.col("clusterId");
        Column accessionIds = unirefRowDataset.col("accessionIds");
        Column uniRefEntry = unirefRowDataset.col("unirefEntry");

        Column joinFilter = array_contains(accessionIds, accession);

        Column oneToManyAggregation = collect_list(accession);

        Dataset<Row> joinedRow = uniprotRowDataset
                .join(unirefRowDataset)
                .where(joinFilter)
                .select(accession, uniprotEntry, uniRefEntry)
                .groupBy(accession, uniprotEntry)
                .agg(accession, uniprotEntry, collect_list(uniRefEntry));


        System.out.println("Joined Rows count: " + joinedRow.count());
        joinedRow.show();
/*          Row[] headRows = (Row[]) joinedRow.head(10);
            for(Row row : headRows){
            System.out.println(" Accession: "+row.getString(row.fieldIndex("accession")));
            System.out.println("Cluster Id: "+row.getString(row.fieldIndex("clusterId")));

            List<String> accessionIdList= row.getList(row.fieldIndex("accessionIds"));
            System.out.println("---------------- Accession Ids: ");
            accessionIdList.forEach(System.out::println);

            byte[] uniprotEntryBytes = (byte[]) row.get(row.fieldIndex("uniprotEntry"));

            //Kryo kryo = new Kryo();
            //kryo.setRegistrationRequired(false);
            //Input entryInput = new Input(1024);
            //entryInput.readBytes(uniprotEntryBytes);
            //UniProtEntry entry = kryo.readObject(entryInput, UniProtEntry.class);
            UniProtEntry entry = SerializationUtils.deserialize(uniprotEntryBytes);
            System.out.println("Accession from Entry: "+entry.getPrimaryAccession().getValue());

            byte[] unirefEntryBytes = (byte[]) row.get(row.fieldIndex("unirefEntry"));
            UniRefEntry unirefEntry = SerializationUtils.deserialize(unirefEntryBytes);
            System.out.println("Cluster Id from entry: "+unirefEntry.getId().getValue());


            System.out.println("----------------------------------------------------");
        }*/


        sc.close();
        System.out.println("**************************************************************");
        System.out.println("******************* ENDED CLOSED SPARK CONFIGURATION *************************");




        /*
        JavaRDD<SolrInputDocument> solrDocuments = convertUniProtEntries(result);
        System.out.println("******************* CREATED SOLR DOCUMENT RDD *************************");
        System.out.println("**************************************************************");
        try {
            SolrSupport.indexDocs("wp-np2-b9:2191", "uniprot", 2500, solrDocuments.rdd());
            System.out.println("******************* INDEXED SOLR DOCUMENT RDD *************************");
            System.out.println("**************************************************************");

            CloudSolrClient solrClient = SolrSupport.getCachedCloudClient("wp-np2-b9:2191");
            solrClient.commit("uniprot");
            System.out.println("******************* COMMITED TO SOLR *************************");
            System.out.println("**************************************************************");
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }*/
    }


    private static JavaRDD<SolrInputDocument> convertUniProtEntries(JavaPairRDD<String, String> entry) {
        //UniprotEntryToSolrDocumentConverter solrDocumentConverter = new UniprotEntryToSolrDocumentConverter();
        //Function<Tuple2<String, UniProtEntry>, SolrInputDocument> fun2 = (tuple2) -> solrDocumentConverter.apply(tuple2._2());
        //return entry.map(fun).map(fun2);
        return null;
    }


}

