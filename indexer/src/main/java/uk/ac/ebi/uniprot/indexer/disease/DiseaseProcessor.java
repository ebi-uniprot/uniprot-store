package uk.ac.ebi.uniprot.indexer.disease;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.cv.disease.Disease;
import uk.ac.ebi.uniprot.domain.builder.DiseaseBuilder;
import uk.ac.ebi.uniprot.indexer.common.utils.DatabaseUtils;
import uk.ac.ebi.uniprot.json.parser.disease.DiseaseJsonConfig;
import uk.ac.ebi.uniprot.search.document.disease.DiseaseDocument;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DiseaseProcessor implements ItemProcessor<Disease, DiseaseDocument> {
    private static final String QUERY_TO_GET_COUNT_PER_DISEASE = "SELECT ENTRY_TYPE, COUNT(ACCESSION)" +
            "  FROM" +
            "  (   " +
            "    SELECT DISTINCT db.ACCESSION, db.ENTRY_TYPE, TRIM(SUBSTR(css.TEXT, 0, INSTR(css.TEXT, ' (') )) DISEASE_IDENTIFIER" +
            "    FROM" +
            "      SPTR.DBENTRY db " +
            "      JOIN SPTR.COMMENT_BLOCK cb ON db.DBENTRY_ID = cb.DBENTRY_ID" +
            "      JOIN SPTR.CV_COMMENT_TOPICS ct ON ct.COMMENT_TOPICS_ID = cb.COMMENT_TOPICS_ID" +
            "      JOIN SPTR.COMMENT_STRUCTURE cs ON cb.COMMENT_BLOCK_ID = cs.COMMENT_BLOCK_ID" +
            "      JOIN SPTR.CV_CC_STRUCTURE_TYPE cst ON cs.CC_STRUCTURE_TYPE_ID = cst.CC_STRUCTURE_TYPE_ID" +
            "      JOIN SPTR.COMMENT_SUBSTRUCTURE css ON cs.COMMENT_STRUCTURE_ID = css.COMMENT_STRUCTURE_ID" +
            "    WHERE ct.TOPIC = 'DISEASE'" +
            "      AND cst.\"TYPE\" = 'DISEASE'" +
            "      AND db.ENTRY_TYPE IN (0,1)" +
            "      AND db.DELETED = 'N'" +
            "      AND db.MERGE_STATUS <> 'R'" +
            "  )" +
            "  WHERE DISEASE_IDENTIFIER = ?" +
            "  GROUP BY DISEASE_IDENTIFIER, ENTRY_TYPE";

    private ObjectMapper diseaseObjectMapper;
    private DatabaseUtils databaseUtils;

    public DiseaseProcessor(DataSource readDataSource) throws SQLException {
        this.databaseUtils = new DatabaseUtils(readDataSource, QUERY_TO_GET_COUNT_PER_DISEASE);
        this.diseaseObjectMapper = DiseaseJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public DiseaseDocument process(Disease disease){
        List<String> kwIds = new ArrayList<>();
        if(disease.getKeywords() != null){
            kwIds = disease.getKeywords().stream().map(kw -> kw.getId()).collect(Collectors.toList());
        }
        // name is a combination of id, acronym, definition, synonyms, keywords
        List<String> name = new ArrayList<>();
        name.add(disease.getId());
        name.add(disease.getAcronym());
        name.add(disease.getDefinition());
        name.addAll(kwIds);
        name.addAll(disease.getAlternativeNames());

        // content is name + accession
        List<String> content = new ArrayList<>();
        content.addAll(name);
        content.add(disease.getAccession());

        // create disease document
        DiseaseDocument.DiseaseDocumentBuilder builder = DiseaseDocument.builder();
        builder.accession(disease.getAccession());
        builder.name(name).content(content);
        byte[] diseaseByte = getDiseaseObjectBinary(disease);
        builder.diseaseObj(ByteBuffer.wrap(diseaseByte));

        return builder.build();
    }

    private byte[] getDiseaseObjectBinary(Disease disease) {
        try {
            // get the protein count, reviewed and unreviewed
            Pair<Long, Long> revUnrevCountPair = this.databaseUtils.getProteinCount(disease.getId());

            DiseaseBuilder diseaseBuilder = DiseaseBuilder.newInstance().from(disease);
            diseaseBuilder.reviewedProteinCount(revUnrevCountPair.getLeft());
            diseaseBuilder.unreviewedProteinCount(revUnrevCountPair.getRight());

            return this.diseaseObjectMapper.writeValueAsBytes(diseaseBuilder.build());

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse disease to binary json: ", e);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to get protein count for disease " + disease.getId(), e);
        }
    }

}
