package uk.ac.ebi.uniprot.indexer.disease;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.MathUtils;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.cv.disease.Disease;
import uk.ac.ebi.uniprot.domain.builder.DiseaseBuilder;
import uk.ac.ebi.uniprot.json.parser.disease.DiseaseJsonConfig;
import uk.ac.ebi.uniprot.search.document.disease.DiseaseDocument;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
    private DataSource readDataSource;
    private Connection dbConnxn;
    private PreparedStatement preparedStatement;

    public DiseaseProcessor(DataSource readDataSource) throws SQLException {
        this.diseaseObjectMapper = DiseaseJsonConfig.getInstance().getFullObjectMapper();
        this.readDataSource = readDataSource;
        this.dbConnxn = this.readDataSource.getConnection();
        this.preparedStatement = this.dbConnxn.prepareStatement(QUERY_TO_GET_COUNT_PER_DISEASE);
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

            Pair<Long, Long> revUnrevCountPair = getProteinCount(disease.getId());

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

    private Pair<Long, Long> getProteinCount(String identifier) throws SQLException {
        this.preparedStatement.setString(1, identifier);
        Long revCount = 0L;
        Long unrevCount = 0L;
        try(ResultSet resultSet = this.preparedStatement.executeQuery()){
            while(resultSet.next()){
                 if(Long.valueOf(0).equals(resultSet.getLong(1))){ // 0 == reviewed, 1 == unreviewed
                     revCount = resultSet.getLong(2);
                 } else if(Long.valueOf(1).equals(resultSet.getLong(1))){
                     unrevCount = resultSet.getLong(2);
                 }

            }
        }
        Pair<Long, Long> revUnrevCountPair = new ImmutablePair<>(revCount, unrevCount);

        return revUnrevCountPair;
    }
}
