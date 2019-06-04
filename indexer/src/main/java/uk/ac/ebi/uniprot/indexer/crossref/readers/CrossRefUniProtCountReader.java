package uk.ac.ebi.uniprot.indexer.crossref.readers;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemReader;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.common.utils.DatabaseUtils;
import uk.ac.ebi.uniprot.search.document.dbxref.CrossRefDocument;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CrossRefUniProtCountReader implements ItemReader<CrossRefDocument> {
    private static final String QUERY_TO_GET_COUNT_PER_ABBR =
            " SELECT cref.entry_type, COUNT(DISTINCT cref.ACCESSION) FROM " +
                    " (" +
                    " SELECT db.* FROM SPTR.DBENTRY db" +
                    " JOIN " +
                    " (" +
                    "   SELECT d2d.DBENTRY_ID FROM SPTR.DBENTRY_2_DATABASE d2d" +
                    "   JOIN SPTR.DATABASE_NAME dn ON d2d.DATABASE_ID = dn.DATABASE_ID" +
                    "   WHERE dn.ABBREVIATION = ?" +
                    "  ) tmp ON db.DBENTRY_ID = tmp.DBENTRY_ID" +
                    " ) cref" +
                    " WHERE" +
                    " cref.entry_type in (0,1)" +
                    " AND cref.deleted ='N'" +
                    " AND cref.merge_status <>'R'" +
                    " GROUP BY cref.entry_type";

    private List<Pair<String, String>> accAbbrPairs;
    private DatabaseUtils databaseUtils;


    public CrossRefUniProtCountReader(DataSource readDataSource) throws SQLException {
        this.databaseUtils = new DatabaseUtils(readDataSource, QUERY_TO_GET_COUNT_PER_ABBR);
        this.accAbbrPairs = new ArrayList<>();
    }

    @Override
    public CrossRefDocument read() throws SQLException {
        CrossRefDocument crossRef = null;

        if (this.accAbbrPairs != null && !this.accAbbrPairs.isEmpty()) {
            Pair<String, String> accAbbr = this.accAbbrPairs.get(0);
            // get the protein count, reviewed and unreviewed
            Pair<Long, Long> revUnrevProtCount = this.databaseUtils.getProteinCount(accAbbr.getRight());

            log.info("The reviewed uniprot count for cross ref {} is {}", accAbbr.getRight(), revUnrevProtCount
                    .getLeft());
            log.info("The unreviewed uniprot count for cross ref {} is {}", accAbbr.getRight(), revUnrevProtCount
                    .getRight());
            CrossRefDocument.CrossRefDocumentBuilder bldr = CrossRefDocument.builder();
            bldr.accession(accAbbr.getLeft());
            bldr.reviewedProteinCount(revUnrevProtCount.getLeft());
            bldr.unreviewedProteinCount(revUnrevProtCount.getRight());
            crossRef = bldr.build();
            // remove the cross ref after use
            this.accAbbrPairs.remove(0);
        }

        return crossRef;
    }

    @BeforeStep
    public void getStepExecution(final StepExecution stepExecution) {

        this.accAbbrPairs = (List<Pair<String, String>>) stepExecution.getJobExecution()
                .getExecutionContext().get(Constants.CROSS_REF_KEY_STR);
    }
}
