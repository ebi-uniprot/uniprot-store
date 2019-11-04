package indexer.uniprot.converter;

import indexer.go.relations.GoRelationFileReader;
import indexer.go.relations.GoRelations;
import indexer.go.relations.GoTerm;
import indexer.go.relations.GoTermFileReader;
import indexer.util.SparkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.pathway.UniPathway;
import org.uniprot.core.cv.pathway.UniPathwayFileReader;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-09-30
 */
@Slf4j
public class UniprotDocumentConverter {

    public static JavaPairRDD<String, UniProtDocument> convert(JavaPairRDD<String, UniProtEntry> uniProtEntryRDD, ResourceBundle applicationConfig, Configuration hadoopConfig) {
        GoRelations goRelations = loadGoRelations(applicationConfig, hadoopConfig);
        Map<String, UniPathway> pathway = loadPathway(applicationConfig, hadoopConfig);
        return (JavaPairRDD<String, UniProtDocument>) uniProtEntryRDD
                .mapValues(new UniProtEntryToSolrDocumentConverter(goRelations, pathway));
    }

    private static GoRelations loadGoRelations(ResourceBundle applicationConfig, Configuration hadoopConfig) {
        String goRelationsFolder = applicationConfig.getString("go.relations.dir.path");
        GoRelations goRelations = new GoRelations();

        GoTermFileReader goTermFileReader = new GoTermFileReader(goRelationsFolder, hadoopConfig);
        List<GoTerm> goTerms = goTermFileReader.read();
        goRelations.addTerms(goTerms);
        log.info("Loaded " + goTerms.size() + " GO relations terms");
        GoRelationFileReader goRelationFileReader = new GoRelationFileReader(goRelationsFolder, hadoopConfig);
        Map<String, Set<String>> relations = goRelationFileReader.read();
        goRelations.addRelations(relations);
        log.info("Loaded " + relations.size() + " GO relations map");
        return goRelations;
    }

    private static Map<String, UniPathway> loadPathway(ResourceBundle applicationConfig, Configuration hadoopConfig) {
        String filePath = applicationConfig.getString("pathway.file.path");
        UniPathwayFileReader uniPathwayFileReader = new UniPathwayFileReader();
        List<String> lines = SparkUtils.readLines(filePath, hadoopConfig);
        List<UniPathway> pathwayList = uniPathwayFileReader.parseLines(lines);
        return pathwayList.stream()
                .collect(Collectors.toMap(UniPathway::getName, java.util.function.Function.identity()));
    }

    private static class UniProtEntryToSolrDocumentConverter implements Serializable, Function<UniProtEntry, UniProtDocument> {

        private static final long serialVersionUID = -6891371730036443245L;
        private final GoRelations goRelations;
        private final Map<String, UniPathway> pathway;

        public UniProtEntryToSolrDocumentConverter(GoRelations goRelations, Map<String, UniPathway> pathway) {
            this.goRelations = goRelations;
            this.pathway = pathway;
            log.info("LOADED SUPORTING DATA UniPathway: " + pathway.size());
            log.info("LOADED SUPORTING DATA Go Relations: " + (goRelations != null));
        }

        @Override
        public UniProtDocument call(UniProtEntry uniProtEntry) throws Exception {
            UniProtEntryConverter converter = new UniProtEntryConverter(goRelations, pathway);
            return converter.convert(uniProtEntry);
        }
    }

}
