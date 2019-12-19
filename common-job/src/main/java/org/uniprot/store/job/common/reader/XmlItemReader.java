package org.uniprot.store.job.common.reader;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.uniprot.core.xml.XmlChainIterator;

/**
 * @author jluo
 * @date: 18 Jun 2019
 */
public class XmlItemReader<T> implements ItemReader<T> {
    protected XmlChainIterator<T, T> entryIterator;

    public XmlItemReader(String filepath, Class<T> clazz, String startElement) {
        init(filepath, clazz, startElement);
    }

    private void init(String filepath, Class<T> clazz, String startElement) {
        File file = new File(filepath);
        List<String> collect = null;

        if (file.isFile()) {
            collect = new ArrayList<>();
            collect.add(filepath);

        } else if (file.isDirectory()) {
            String[] list =
                    file.list(
                            (dir, name) ->
                                    name.endsWith(".xml")
                                            || name.endsWith(".xml.gz")
                                            || name.endsWith(".xml.gzip"));

            if (list.length == 0) {
                throw new RuntimeException("No xml files exists in the specified directory");
            }

            collect =
                    Arrays.asList(list).stream()
                            .map((fi) -> (filepath + "/" + fi))
                            .collect(Collectors.toList());

        } else {
            throw new RuntimeException("Please specify the directory that contains the xml files");
        }
        entryIterator =
                new XmlChainIterator<>(
                        new XmlChainIterator.FileInputStreamIterator(collect),
                        clazz,
                        startElement,
                        Function.identity());
    }

    @Override
    public T read()
            throws Exception, UnexpectedInputException, ParseException,
                    NonTransientResourceException {
        if (entryIterator.hasNext()) return entryIterator.next();
        else return null;
    }
}
