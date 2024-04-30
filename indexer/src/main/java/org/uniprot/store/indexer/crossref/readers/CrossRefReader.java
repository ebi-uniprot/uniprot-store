package org.uniprot.store.indexer.crossref.readers;

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

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemReader;
import org.uniprot.core.cv.xdb.CrossRefEntry;
import org.uniprot.core.cv.xdb.impl.CrossRefEntryBuilder;
import org.uniprot.core.impl.StatisticsBuilder;
import org.uniprot.store.indexer.common.utils.Constants;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CrossRefReader implements ItemReader<CrossRefEntry> {
    private static final String DATA_REGION_SEP =
            "___________________________________________________________________________";
    private static final String COPYRIGHT_SEP =
            "-----------------------------------------------------------------------";
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
    private static final String REF_TYPE_DOI = "DOI";
    private static final String REF_TYPE_PUBMED = "PubMed";
    private static final String IMPLICIT = "Implicit";

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
    public void getCrossRefProteinCountMap(
            final StepExecution stepExecution) { // get the cached data from previous step

        this.crossRefProteinCountMap =
                (Map<String, CrossRefUniProtCountReader.CrossRefProteinCount>)
                        stepExecution
                                .getJobExecution()
                                .getExecutionContext()
                                .get(Constants.CROSS_REF_PROTEIN_COUNT_KEY);
    }

    private CrossRefEntry convertToDBXRef(String linesStr) {
        String[] lines = linesStr.split(LINE_SEP);
        boolean processingServer = false;
        CrossRefEntryBuilder builder = new CrossRefEntryBuilder();
        String abbr = null;
        for (String line : lines) {
            String[] keyVal = line.split(KEY_VAL_SEPARATOR);
            switch (keyVal[0].trim()) {
                case AC_STR:
                    builder.id(keyVal[1].trim());
                    processingServer = false;
                    break;
                case ABBREV_STR:
                    abbr = keyVal[1].trim();
                    builder.abbrev(abbr);
                    processingServer = false;
                    break;
                case NAME_STR:
                    builder.name(keyVal[1].trim());
                    processingServer = false;
                    break;
                case REF_STR:
                    String[] refIdPairs = keyVal[1].trim().split(REF_SEPARATOR);
                    if (refIdPairs.length == 2) {
                        String pubMedId =
                                refIdPairs[0]
                                        .split(EQUAL_CHAR)[1]
                                        .replace(SEMI_COLON, EMPTY_CHAR)
                                        .trim();
                        builder.pubMedId(pubMedId);
                        String doiId =
                                refIdPairs[1]
                                        .split(EQUAL_CHAR)[1]
                                        .replace(SEMI_COLON, EMPTY_CHAR)
                                        .trim();
                        builder.doiId(doiId);
                    } else if (refIdPairs.length == 1) {
                        String refIdType = refIdPairs[0].split(EQUAL_CHAR)[0].trim();
                        if (REF_TYPE_DOI.equalsIgnoreCase(refIdType)) {
                            String doiId =
                                    refIdPairs[0]
                                            .split(EQUAL_CHAR)[1]
                                            .replace(SEMI_COLON, EMPTY_CHAR)
                                            .trim();
                            builder.doiId(doiId);
                        } else if (REF_TYPE_PUBMED.equalsIgnoreCase(refIdType)) {
                            String pubMedId =
                                    refIdPairs[0]
                                            .split(EQUAL_CHAR)[1]
                                            .replace(SEMI_COLON, EMPTY_CHAR)
                                            .trim();
                            builder.pubMedId(pubMedId);
                        }
                    }
                    processingServer = false;
                    break;
                case LINK_TP_STR:
                    String lType = keyVal[1].trim();
                    if (lType.startsWith(IMPLICIT)) {
                        lType = IMPLICIT;
                    }
                    builder.linkType(lType);
                    processingServer = false;
                    break;
                case SERVER_STR:
                    builder.serversAdd(keyVal[1].trim());
                    processingServer = true;
                    break;
                case DB_URL_STR:
                    builder.dbUrl(keyVal[1].trim());
                    processingServer = false;
                    break;
                case CAT_STR:
                    builder.category(keyVal[1].trim());
                    processingServer = false;
                    break;
                default:
                    if (processingServer) {
                        builder.serversAdd(keyVal[0].trim());
                    }
                    break;
            }
        }

        // update the reviewed and unreviewed protein count
        CrossRefUniProtCountReader.CrossRefProteinCount crossRefProteinCount =
                this.crossRefProteinCountMap.get(abbr);

        if (crossRefProteinCount != null) {
            StatisticsBuilder statBuilder = new StatisticsBuilder();
            statBuilder.reviewedProteinCount(crossRefProteinCount.getReviewedProteinCount());
            statBuilder.unreviewedProteinCount(crossRefProteinCount.getUnreviewedProteinCount());
            builder.statistics(statBuilder.build());
        } else {
            log.warn("Cross ref with abbreviation {} not in the uniprot db", abbr);
        }

        return builder.build();
    }
}
