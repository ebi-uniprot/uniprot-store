package uk.ac.ebi.uniprot.indexer.crossref.readers;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemReader;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.search.document.dbxref.CrossRefDocument;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Slf4j
public class CrossRefUniProtCountReader implements ItemReader<CrossRefDocument> {
    private static final String QUERY_TO_GET_COUNT_PER_ABBR =
            " SELECT COUNT(DISTINCT cref.ACCESSION) FROM " +
                " (" +
                    " SELECT * FROM DBENTRY db" +
                    " JOIN " +
                    " (" +
                    "   SELECT d2d.DBENTRY_ID FROM DBENTRY_2_DATABASE d2d" +
                    "   JOIN DATABASE_NAME dn ON d2d.DATABASE_ID = dn.DATABASE_ID" +
                    "   WHERE dn.ABBREVIATION = ?" +
                    "  ) tmp ON db.DBENTRY_ID = tmp.DBENTRY_ID" +
                " ) cref" +
                " WHERE" +
                " cref.entry_type in (0,1)" +
                " AND cref.deleted ='N'" +
                " AND cref.merge_status <>'R'";

    private List<Pair<String, String>> accAbbrPairs;
    private DataSource readDataSource;
    private Connection dbConnxn;
    private PreparedStatement pStat;


    public CrossRefUniProtCountReader(DataSource readDataSource) throws SQLException {
        this.readDataSource = readDataSource;
        this.dbConnxn = this.readDataSource.getConnection();
        this.pStat = this.dbConnxn.prepareStatement(QUERY_TO_GET_COUNT_PER_ABBR);
    }

    @Override
    public CrossRefDocument read() throws SQLException {
        CrossRefDocument crossRef = null;

        if(this.accAbbrPairs != null || !this.accAbbrPairs.isEmpty()) {
            Pair<String, String> accAbbr = this.accAbbrPairs.get(0);
            Long uniprotCount = getUniProtCount(accAbbr.getRight());
            log.info("The uniprot count for cross ref {} is {}", accAbbr.getRight(), uniprotCount);
            CrossRefDocument.CrossRefDocumentBuilder bldr = CrossRefDocument.builder();
            bldr.accession(accAbbr.getLeft());
            bldr.uniprotCount(uniprotCount);
            crossRef = bldr.build();
            // remove the cross ref after use
            this.accAbbrPairs.remove(0);
        }

        return crossRef;
    }

    @BeforeStep
    public void getStepExecution(final StepExecution stepExecution){

        this.accAbbrPairs = (List<Pair<String, String>>) stepExecution.getJobExecution()
                .getExecutionContext().get(Constants.CROSS_REF_KEY_STR);
    }

    private Long getUniProtCount(String abbrev) throws SQLException {
        this.pStat.setString(1, abbrev);

        try(ResultSet resultSet = this.pStat.executeQuery()){
            while(resultSet.next()){
                return resultSet.getLong(1);
            }
        }

        return null;
    }

}
