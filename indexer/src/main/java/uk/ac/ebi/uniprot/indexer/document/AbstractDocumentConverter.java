package uk.ac.ebi.uniprot.indexer.document;


/**
 * @author wudong
 */
public abstract class AbstractDocumentConverter<S, T extends Document> implements DocumentConverter<S, T> {

//    private DocumentTypeEnum supportedType = null;
//
//    @Override
//    public DocumentTypeEnum getDocumentType() {
//        if (supportedType == null) {
//            SupportedDocumentType annotation = this.getClass().getAnnotation(SupportedDocumentType.class);
//            if (annotation != null) {
//                DocumentTypeEnum[] types = annotation.types();
//                if (types != null && types.length == 1) {
//                    supportedType = types[0];
//                }
//            }
//
//            if (supportedType == null) {
//                throw new RuntimeException("Not Document Type is specified.");
//            }
//        }
//        return supportedType;
//    }

    protected void sourcePreconditionCheck(S source) {
        if (source == null) {
            throw new DocumentConversionException("Provided source is null");
        }
    }
}
