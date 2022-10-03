package org.uniprot.store.spark.indexer.publication.mapper;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.ac.AcLineObject;
import org.uniprot.core.flatfile.parser.impl.ox.OxLineObject;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryReferencesConverter;
import org.uniprot.store.spark.indexer.common.converter.UniProtKBReferencesConverter;

import scala.Tuple2;

/**
 * Maps an entry string to an iterator of tuples with values <accession_citationId,
 * MappedReference>.
 *
 * <p>Created 18/01/2021
 *
 * @author Edd
 */
public class UniProtKBPublicationToMappedReference
        implements PairFlatMapFunction<String, String, MappedReference> {

    private static final Pattern REVIEWED_REGEX = Pattern.compile("^ID .*Reviewed.*");
    private static final long serialVersionUID = -755120294877372128L;
    private final UniProtKBReferencesConverter uniProtKBReferencesConverter =
            new UniProtKBReferencesConverter();

    private final UniProtEntryReferencesConverter referencesConverter =
            new UniProtEntryReferencesConverter();

    @Override
    public Iterator<Tuple2<String, MappedReference>> call(String entryStr) throws Exception {
        String[] lines = entryStr.split("\n");

        String accession = getAccession(lines);
        UniProtKBEntryType entryType = getEntryType(lines);
        long organismId = getOrganismId(lines);
        List<UniProtKBReference> references = uniProtKBReferencesConverter.convert(lines);

        AtomicInteger refNumberCounter = new AtomicInteger();
        return references.stream()
                .map(
                        ref ->
                                createMappedReferenceInfo(
                                        accession,
                                        entryType,
                                        ref,
                                        organismId,
                                        refNumberCounter.getAndIncrement()))
                .map(
                        referenceInfo ->
                                new Tuple2<>(
                                        accession + "_" + referenceInfo.citationId,
                                        referenceInfo.mappedReference))
                .iterator();
    }

    String getAccession(String[] lines) {
        final UniprotKBLineParser<AcLineObject> acParser =
                new DefaultUniprotKBLineParserFactory().createAcLineParser();

        StringBuilder sb = new StringBuilder();
        boolean foundAccession = false;
        for (String line : lines) {
            boolean lineStartsWithAC = line.startsWith("AC ");
            if (lineStartsWithAC) {
                sb.append(line).append('\n');
                foundAccession = true;
            }
            if (foundAccession && !lineStartsWithAC) {
                break;
            }
        }

        return acParser.parse(sb.toString() + "\n").primaryAcc;
    }

    long getOrganismId(String[] lines) {
        final UniprotKBLineParser<OxLineObject> oxParser =
                new DefaultUniprotKBLineParserFactory().createOxLineParser();
        long organismId = 0L;

        for (String line : lines) {
            boolean lineStartsWithOx = line.startsWith("OX  ");
            if (lineStartsWithOx) {
                organismId = oxParser.parse(line + "\n").taxonomy_id;
            }
            if (organismId > 0) {
                break;
            }
        }
        return organismId;
    }

    static class MappedReferenceInfo {
        MappedReference mappedReference;
        String citationId;
    }

    MappedReferenceInfo createMappedReferenceInfo(
            String accession,
            UniProtKBEntryType entryType,
            UniProtKBReference reference,
            long organismId,
            int referenceNumber) {

        String citationId = reference.getCitation().getId();

        MappedReferenceInfo mappedReferenceInfo = new MappedReferenceInfo();
        mappedReferenceInfo.mappedReference =
                referencesConverter.createUniProtKBMappedReference(
                        accession, entryType, reference, citationId, organismId, referenceNumber);
        mappedReferenceInfo.citationId = citationId;
        return mappedReferenceInfo;
    }

    private UniProtKBEntryType getEntryType(String[] lines) {
        if (REVIEWED_REGEX.matcher(lines[0]).matches()) {
            return UniProtKBEntryType.SWISSPROT;
        } else {
            return UniProtKBEntryType.TREMBL;
        }
    }
}
