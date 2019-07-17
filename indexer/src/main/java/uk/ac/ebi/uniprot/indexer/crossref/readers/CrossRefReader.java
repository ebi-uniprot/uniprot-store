package uk.ac.ebi.uniprot.indexer.crossref.readers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemReader;
import uk.ac.ebi.uniprot.domain.crossref.CrossRefEntry;
import uk.ac.ebi.uniprot.domain.crossref.CrossRefEntryBuilder;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

@Slf4j
public class CrossRefReader implements ItemReader<CrossRefEntry> {
    private static final String DATA_REGION_SEP = "___________________________________________________________________________";
    private static final String COPYRIGHT_SEP = "-----------------------------------------------------------------------";
    private static final String LINE_SEP = "\n";
    private static final char SEMI_COLON = ';';
    private static final char EMPTY_CHAR = ' ';
    private static final String EQUAL_CHAR = "=";
    // Fields of the dbxref file - begin
    private static final String AC_STR = "AC";
    private static final String ABBREV_STR = "Abbrev";
    private static final String NAME_STR = "Name";
    private static final String REF_STR = "Ref";
    private static final String LINK_TP_STR = "LinkTp";
    private static final String SERVER_STR = "Server";
    private static final String DB_URL_STR = "Db_URL";
    private static final String CAT_STR = "Cat";
    // Fields of the dbxref file - end

    private static final String KEY_VAL_SEPARATOR = ": ";
    private static final String REF_SEPARATOR = " ";
    private static final Pattern NEWLINE_PATTERN = Pattern.compile("^\\s*$", Pattern.MULTILINE);
    private static final String FTP_PREFIX = "ftp://";
    private static final String UNDER_ZERO = "_0";
    private static final String UNDER_ONE = "_1";

    private Scanner reader;
    private boolean dataRegionStarted;

    // cache to be loaded from context of previous step, see method getStepExecution below
    private Map<String, CrossRefUniProtCountReader.CrossRefProteinCount> crossRefProteinCountMap;

    public CrossRefReader(String filePath) throws IOException {
        InputStream inputStream;

        if (filePath.startsWith(FTP_PREFIX)) {
            URL url = new URL(filePath);
            URLConnection conn = url.openConnection();
            inputStream = conn.getInputStream();
        } else {
            inputStream = this.getClass().getClassLoader().getResourceAsStream(filePath);

            if (inputStream == null) {
                inputStream = new FileInputStream(new File(filePath));
            }
        }

        this.reader = new Scanner(inputStream, StandardCharsets.UTF_8.name());
        this.dataRegionStarted = false;
    }

    @Override
    public CrossRefEntry read() {
        if (this.reader.hasNext()) {
            // skip the un-needed lines
            while (this.reader.hasNext() && !this.dataRegionStarted) {
                String lines = reader.next();
                if (DATA_REGION_SEP.equals(lines)) {
                    this.dataRegionStarted = true;
                    this.reader.useDelimiter(NEWLINE_PATTERN);
                }
            }

            CrossRefEntry dbxRef = null;
            String lines = this.reader.next();
            if (!lines.contains(COPYRIGHT_SEP)) {
                dbxRef = convertToDBXRef(lines);
            }

            return dbxRef;
        } else {
            return null;
        }
    }

    @BeforeStep
    public void getCrossRefProteinCountMap(final StepExecution stepExecution) {// get the cached data from previous step

        this.crossRefProteinCountMap = (Map<String, CrossRefUniProtCountReader.CrossRefProteinCount>) stepExecution.getJobExecution()
                .getExecutionContext().get(Constants.CROSS_REF_PROTEIN_COUNT_KEY);
    }

    private CrossRefEntry convertToDBXRef(String linesStr) {
        String[] lines = linesStr.split(LINE_SEP);
        String acc = null, abbr = null, name = null, pubMedId = null, doiId = null;
        String lType = null, server = null, url = null, cat = null;

        for (String line : lines) {
            String[] keyVal = line.split(KEY_VAL_SEPARATOR);
            switch (keyVal[0].trim()) {
                case AC_STR:
                    acc = keyVal[1].trim();
                    break;
                case ABBREV_STR:
                    abbr = keyVal[1].trim();
                    break;
                case NAME_STR:
                    name = keyVal[1].trim();
                    break;
                case REF_STR:
                    String[] refIdPairs = keyVal[1].trim().split(REF_SEPARATOR);
                    assert refIdPairs.length == 2;
                    pubMedId = refIdPairs[0].split(EQUAL_CHAR)[1].replace(SEMI_COLON, EMPTY_CHAR).trim();
                    doiId = refIdPairs[1].split(EQUAL_CHAR)[1].replace(SEMI_COLON, EMPTY_CHAR).trim();
                    break;
                case LINK_TP_STR:
                    lType = keyVal[1].trim();
                    break;
                case SERVER_STR:
                    server = keyVal[1].trim();
                    break;
                case DB_URL_STR:
                    url = keyVal[1].trim();
                    break;
                case CAT_STR:
                    cat = keyVal[1].trim();
                    break;
                default://do nothing
            }
        }

        CrossRefEntryBuilder builder = new CrossRefEntryBuilder();
        builder.accession(acc).abbrev(abbr).name(name);
        builder.pubMedId(pubMedId).doiId(doiId).linkType(lType).server(server);
        builder.dbUrl(url).category(cat);

        // update the reviewed and unreviewed protein count
        CrossRefUniProtCountReader.CrossRefProteinCount reviewedProtCount = this.crossRefProteinCountMap.get(abbr + UNDER_ZERO);
        CrossRefUniProtCountReader.CrossRefProteinCount unreviewedProtCount = this.crossRefProteinCountMap.get(abbr + UNDER_ONE);

        if(reviewedProtCount != null){
            builder.reviewedProteinCount(reviewedProtCount.getProteinCount());
        } else {
            log.warn("Cross ref with abbreviation {} not in the uniprot db reviewed", abbr);
        }

        if(unreviewedProtCount != null){
            builder.unreviewedProteinCount(unreviewedProtCount.getProteinCount());
        } else {
            log.warn("Cross ref with abbreviation {} not in the uniprot db unreviewed", abbr);
        }
        return builder.build();
    }
}
