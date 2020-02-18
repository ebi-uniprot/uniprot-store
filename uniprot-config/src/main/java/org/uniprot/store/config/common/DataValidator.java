package org.uniprot.store.config.common;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.common.ConfigFieldValidationException;
import org.uniprot.store.config.model.ConfigFieldItem;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataValidator {
    
    public static void validateContent(List<ConfigFieldItem> fieldItems, Map<String, ConfigFieldItem> idFieldMap){
        validateParentExists(fieldItems, idFieldMap);
        validateSeqNumbers(fieldItems);
        validateChildNumbers(fieldItems);
    }

    private static void validateParentExists(
            List<ConfigFieldItem> fieldItems, Map<String, ConfigFieldItem> idFieldMap) {
        fieldItems.stream()
                .filter(fi -> StringUtils.isNotBlank(fi.getParentId()))
                .forEach(
                        fieldItem -> {
                            if (!idFieldMap.containsKey(fieldItem.getParentId())) {
                                throw new ConfigFieldValidationException(
                                        "Field Item doesn't exist for parentId '"
                                                + fieldItem.getParentId()
                                                + "'");
                            }
                        });
    }

    private static void validateSeqNumbers(List<ConfigFieldItem> fieldItems){
        List<Integer> seqNumbers = extractSeqNumbers(fieldItems);
        validateNaturalNumbers(seqNumbers, "seqNumber");
    }

    private static List<Integer> extractSeqNumbers(List<ConfigFieldItem> fieldItems){
        List<Integer> seqNumbers = fieldItems.stream()
                .filter(fi -> fi.getSeqNumber() != null)
                .map(fi -> fi.getSeqNumber())
                .collect(Collectors.toList());
        return seqNumbers;
    }

    private static void validateChildNumbers(List<ConfigFieldItem> fieldItems) {

        Map<String, List<ConfigFieldItem>> parentChildrenMap = fieldItems.stream()
                .filter(fi -> StringUtils.isNotBlank(fi.getParentId()))
                .collect(Collectors.groupingBy(ConfigFieldItem::getParentId));

        parentChildrenMap.entrySet().stream().forEach(pc -> validateChildNumbers(pc.getKey(), pc.getValue()));

    }

    static void validateChildNumbers(String parentId, List<ConfigFieldItem> children){
        List<Integer> childNumbers = children.stream().map(c -> c.getChildNumber()).collect(Collectors.toList());
        String message = "childNumber for parentId '" + parentId +"'";
        validateNaturalNumbers(childNumbers, message);
    }

    private static void validateNaturalNumbers(List<Integer> numbers, String message){
        // check numbers are natural number including 0
        int inputSize = numbers.size();
        BitSet visitedSet = new BitSet(inputSize);

        for(Integer number : numbers){
            if(number >= inputSize){
                throw new ConfigFieldValidationException(message + " " + number +" is bigger than available number.");
            }
            if(visitedSet.get(number)){
                throw new ConfigFieldValidationException(message + " " + number +" is already used.");
            }
            visitedSet.set(number);
        }
    }
}
