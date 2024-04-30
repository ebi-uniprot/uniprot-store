package org.uniprot.store.spark.indexer.uniref.converter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.util.DateUtils;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.uniref.UniRefDocument;

import lombok.extern.slf4j.Slf4j;

/**
 * This class convert an UniRefEntry to UniRefDocument
 *
 * @author lgonzales
 * @since 2020-02-07
 */
@Slf4j
public class UniRefDocumentConverter
        implements DocumentConverter<UniRefEntry, UniRefDocument>, Serializable {

    private static final long serialVersionUID = -1071849666501652399L;

    @Override
    public UniRefDocument convert(UniRefEntry entry) {
        UniRefDocument.UniRefDocumentBuilder builder = UniRefDocument.builder();
        builder.id(entry.getId().getValue())
                .identity(entry.getEntryType().getIdentity())
                .name(entry.getName())
                .count(entry.getMembers().size() + 1)
                .length(entry.getRepresentativeMember().getSequence().getLength())
                .created(DateUtils.convertLocalDateToUTCDate(entry.getUpdated()))
                .createdSort(DateUtils.convertLocalDateToUTCDate(entry.getUpdated()))
                .organismSort(getOrganismNameForSort(entry))
                .taxLineageId((int) entry.getRepresentativeMember().getOrganismTaxId())
                .organismTaxon(entry.getRepresentativeMember().getOrganismName());

        convertMember(entry.getRepresentativeMember(), builder);
        entry.getMembers().forEach(member -> convertMember(member, builder));

        return builder.build();
    }

    private void convertMember(UniRefMember member, UniRefDocument.UniRefDocumentBuilder builder) {
        // cluster
        if (Utils.notNull(member.getUniRef50Id())) {
            builder.cluster(member.getUniRef50Id().getValue());
        }
        if (Utils.notNull(member.getUniRef90Id())) {
            builder.cluster(member.getUniRef90Id().getValue());
        }
        if (Utils.notNull(member.getUniRef100Id())) {
            builder.cluster(member.getUniRef100Id().getValue());
        }

        if (member.getMemberIdType() == UniRefMemberIdType.UNIPARC) {
            builder.upid(member.getMemberId());
        } else {
            builder.uniprotId(member.getMemberId());
        }
        if (Utils.notNull(member.getUniParcId())) {
            builder.upid(member.getUniParcId().getValue());
        }
        if (Utils.notNull(member.getUniProtAccessions())) {
            member.getUniProtAccessions().forEach(val -> builder.uniprotId(val.getValue()));
        }
    }

    private String getOrganismNameForSort(UniRefEntry entry) {
        List<String> result = new ArrayList<>();
        result.add(entry.getRepresentativeMember().getOrganismName());
        entry.getMembers().stream()
                .map(UniRefMember::getOrganismName)
                .distinct()
                .limit(5)
                .forEach(result::add);
        return String.join(" ", result);
    }
}
