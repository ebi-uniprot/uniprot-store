package org.uniprot.store.config.searchfield.schema;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.searchfield.common.SearchFieldValidationException;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;

public class DataValidator {

    public static void validateContent(
            List<SearchFieldItem> fieldItems, Map<String, SearchFieldItem> idFieldMap) {
        validateParentExists(fieldItems, idFieldMap);
        validateSeqNumbers(fieldItems);
        validateChildNumbers(fieldItems);
        validateSortFieldIds(fieldItems, idFieldMap);
    }

    public static void validateParentExists(
            List<SearchFieldItem> fieldItems, Map<String, SearchFieldItem> idFieldMap) {
        fieldItems.stream()
                .filter(fi -> StringUtils.isNotBlank(fi.getParentId()))
                .forEach(
                        fieldItem -> {
                            if (!idFieldMap.containsKey(fieldItem.getParentId())) {
                                throw new SearchFieldValidationException(
                                        "Field Item doesn't exist for parentId '"
                                                + fieldItem.getParentId()
                                                + "'");
                            }
                        });
    }

    public static void validateSeqNumbers(List<SearchFieldItem> fieldItems) {
        List<Integer> seqNumbers = extractSeqNumbers(fieldItems);
        validateNaturalNumbers(seqNumbers, "seqNumber");
    }

    private static List<Integer> extractSeqNumbers(List<SearchFieldItem> fieldItems) {
        List<Integer> seqNumbers =
                fieldItems.stream()
                        .filter(fi -> fi.getSeqNumber() != null)
                        .map(SearchFieldItem::getSeqNumber)
                        .collect(Collectors.toList());
        return seqNumbers;
    }

    public static void validateChildNumbers(List<SearchFieldItem> fieldItems) {

        Map<String, List<SearchFieldItem>> parentChildrenMap =
                fieldItems.stream()
                        .filter(fi -> StringUtils.isNotBlank(fi.getParentId()))
                        .collect(Collectors.groupingBy(SearchFieldItem::getParentId));

        parentChildrenMap.entrySet().stream()
                .forEach(pc -> validateChildNumbers(pc.getKey(), pc.getValue()));
    }

    public static void validateSortFieldIds(
            List<SearchFieldItem> fieldItems, Map<String, SearchFieldItem> idFieldMap) {
        fieldItems.stream()
                .filter(fi -> hasSortFieldId(fi))
                .forEach(
                        fi -> {
                            if (!idFieldMap.containsKey(fi.getSortFieldId())) {
                                throw new SearchFieldValidationException(
                                        "No field item with id for sortId " + fi.getSortFieldId());
                            }
                        });
    }

    private static void validateChildNumbers(String parentId, List<SearchFieldItem> children) {
        List<Integer> childNumbers =
                children.stream().map(c -> c.getChildNumber()).collect(Collectors.toList());
        String message = "childNumber for parentId '" + parentId + "'";
        validateNaturalNumbers(childNumbers, message);
    }

    private static void validateNaturalNumbers(List<Integer> numbers, String message) {
        // check numbers are natural number including 0
        int inputSize = numbers.size();
        BitSet visitedSet = new BitSet(inputSize);

        for (Integer number : numbers) {
            if (number == null) {
                throw new SearchFieldValidationException(message + " " + number + " is null.");
            }
            if (number >= inputSize) {
                throw new SearchFieldValidationException(
                        message + " " + number + " is bigger than available number.");
            }
            if (number < 0) {
                throw new SearchFieldValidationException(
                        message + " " + number + " is less than zero.");
            }
            if (visitedSet.get(number)) {
                throw new SearchFieldValidationException(
                        message + " " + number + " is already used.");
            }
            visitedSet.set(number);
        }
    }

    private static boolean hasSortFieldId(SearchFieldItem fi) {
        return Objects.nonNull(fi.getSortFieldId());
    }
}
