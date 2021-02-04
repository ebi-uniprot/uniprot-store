package org.uniprot.store.spark.indexer.publication.mapper;

import static org.uniprot.store.spark.indexer.publication.PublicationDocumentsToHDFSWriter.getJoinKey;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.ac.AcLineObject;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObject;
import org.uniprot.core.flatfile.parser.impl.entry.ReferenceObjectConverter;
import org.uniprot.core.flatfile.parser.impl.ra.RaLineObject;
import org.uniprot.core.flatfile.parser.impl.rc.RcLineObject;
import org.uniprot.core.flatfile.parser.impl.rg.RgLineObject;
import org.uniprot.core.flatfile.parser.impl.rl.RlLineObject;
import org.uniprot.core.flatfile.parser.impl.rn.RnLineObject;
import org.uniprot.core.flatfile.parser.impl.rp.RpLineObject;
import org.uniprot.core.flatfile.parser.impl.rt.RtLineObject;
import org.uniprot.core.flatfile.parser.impl.rx.RxLineObject;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryReferencesConverter;

import scala.Tuple2;

/**
 * Maps an entry string to an iterator of tuples with values <accession, MappedReference>.
 *
 * <p>Created 18/01/2021
 *
 * @author Edd
 */
public class UniProtKBPublicationToMappedReference
        implements PairFlatMapFunction<String, String, MappedReference> {

    private static final Pattern REVIEWED_REGEX = Pattern.compile("^ID .*Reviewed.*");
    private static final Pattern REFERENCE_REGEX = Pattern.compile("^(RN|RA|RC|RG|RL|RP|RT|RX) .*");
    private static final long serialVersionUID = -755120294877372128L;
    private final UniProtEntryReferencesConverter referencesConverter =
            new UniProtEntryReferencesConverter();

    @Override
    public Iterator<Tuple2<String, MappedReference>> call(String entryStr) throws Exception {
        String[] lines = entryStr.split("\n");

        String accession = getAccession(lines);
        UniProtKBEntryType entryType = getEntryType(lines);
        List<UniProtKBReference> references = createReferences(lines);

        AtomicInteger refNumberCounter = new AtomicInteger();
        return references.stream()
                .map(
                        ref ->
                                createMappedReferenceInfo(
                                        accession,
                                        entryType,
                                        ref,
                                        refNumberCounter.getAndIncrement()))
                .map(
                        referenceInfo ->
                                new Tuple2<>(
                                        getJoinKey(accession, referenceInfo.pubMed),
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

    List<UniProtKBReference> createReferences(String[] lines) {
        // for each reference in the entry, create a list of lines related to it
        List<List<String>> rawRefs = new ArrayList<>();
        List<String> rawRef = new ArrayList<>();
        for (String line : lines) {
            if (REFERENCE_REGEX.matcher(line).matches()) {
                if (line.startsWith("RN ")) {
                    if (!rawRef.isEmpty()) {
                        rawRefs.add(rawRef);
                    }
                    rawRef = new ArrayList<>();
                }

                rawRef.add(line + "\n");
            }
        }

        rawRefs.add(rawRef);

        return rawRefs.stream()
                .map(this::getSections)
                .map(this::createReference)
                .collect(Collectors.toList());
    }

    List<LineTypeSection> getSections(List<String> lines) {
        List<LineTypeSection> lineTypeSections = new ArrayList<>();

        boolean isNotLastSection = true;
        int offset = 0;
        while (isNotLastSection) {
            LineTypeSection section = getSectionWithSameLineType(lines, offset);
            lineTypeSections.add(section);
            isNotLastSection = section.isNotLastSection;
            offset = section.lineNumber;
        }

        return lineTypeSections;
    }

    LineTypeSection getSectionWithSameLineType(List<String> lines, int offset) {
        LineTypeSection lineTypeSection = new LineTypeSection();

        LineType lineType = null;
        StringBuilder sb = new StringBuilder();
        int i = offset;
        for (; i < lines.size(); i++) {
            String line = lines.get(i);

            lineType = LineType.valueOf(lines.get(i).substring(0, 2));
            if (lineTypeSection.type == null) {
                lineTypeSection.type = lineType;
            } else if (lineType != lineTypeSection.type) {
                break;
            }

            sb.append(line);
        }

        lineTypeSection.lineNumber = i;
        lineTypeSection.value = sb.toString();
        if (i == lines.size()) {
            lineTypeSection.isNotLastSection = false;
        }

        return lineTypeSection;
    }

    static class MappedReferenceInfo {
        MappedReference mappedReference;
        String pubMed;
    }

    MappedReferenceInfo createMappedReferenceInfo(
            String accession,
            UniProtKBEntryType entryType,
            UniProtKBReference reference,
            int referenceNumber) {

        String pubMed = referencesConverter.extractPubMed(reference.getCitation());

        MappedReferenceInfo mappedReferenceInfo = new MappedReferenceInfo();
        mappedReferenceInfo.mappedReference =
                referencesConverter.createUniProtKBMappedReference(
                        accession, entryType, reference, pubMed, referenceNumber);
        mappedReferenceInfo.pubMed = pubMed;
        return mappedReferenceInfo;
    }

    UniProtKBReference createReference(List<LineTypeSection> sections) {
        final UniprotKBLineParser<RaLineObject> raLineParser =
                new DefaultUniprotKBLineParserFactory().createRaLineParser();
        final UniprotKBLineParser<RcLineObject> rcLineParser =
                new DefaultUniprotKBLineParserFactory().createRcLineParser();
        final UniprotKBLineParser<RgLineObject> rgLineParser =
                new DefaultUniprotKBLineParserFactory().createRgLineParser();
        final UniprotKBLineParser<RlLineObject> rlLineParser =
                new DefaultUniprotKBLineParserFactory().createRlLineParser();
        final UniprotKBLineParser<RnLineObject> rnLineParser =
                new DefaultUniprotKBLineParserFactory().createRnLineParser();
        final UniprotKBLineParser<RpLineObject> rpLineParser =
                new DefaultUniprotKBLineParserFactory().createRpLineParser();
        final UniprotKBLineParser<RtLineObject> rtLineParser =
                new DefaultUniprotKBLineParserFactory().createRtLineParser();
        final UniprotKBLineParser<RxLineObject> rxLineParser =
                new DefaultUniprotKBLineParserFactory().createRxLineParser();

        ReferenceObjectConverter referenceObjectConverter = new ReferenceObjectConverter();
        EntryObject.ReferenceObject referenceObject = new EntryObject.ReferenceObject();

        for (LineTypeSection section : sections) {
            switch (section.type) {
                case RA:
                    referenceObject.ra = raLineParser.parse(section.value);
                    break;
                case RC:
                    referenceObject.rc = rcLineParser.parse(section.value);
                    break;
                case RG:
                    referenceObject.rg = rgLineParser.parse(section.value);
                    break;
                case RL:
                    referenceObject.rl = rlLineParser.parse(section.value);
                    break;
                case RN:
                    referenceObject.rn = rnLineParser.parse(section.value);
                    break;
                case RP:
                    referenceObject.rp = rpLineParser.parse(section.value);
                    break;
                case RT:
                    referenceObject.rt = rtLineParser.parse(section.value);
                    break;
                case RX:
                    referenceObject.rx = rxLineParser.parse(section.value);
                    break;
                default:
                    throw new IllegalStateException(
                            "Unexpected line type encountered: " + section.value);
            }
        }

        return referenceObjectConverter.convert(referenceObject);
    }

    private UniProtKBEntryType getEntryType(String[] lines) {
        if (REVIEWED_REGEX.matcher(lines[0]).matches()) {
            return UniProtKBEntryType.SWISSPROT;
        } else {
            return UniProtKBEntryType.TREMBL;
        }
    }

    /** Represents all lines of the same type (e.g., RT) of a UniProtKB entry flatfile. */
    static class LineTypeSection {
        int lineNumber = 0;
        LineType type;
        String value;
        boolean isNotLastSection = true;
    }
}
