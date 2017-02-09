package pt.ua.deti.ir.Structures;

import java.util.List;

public class Document {

    private final Integer docID;
    private List<String> content;

    public Document(Integer docID, List<String> content) {
        this.docID = docID;
        this.content = content;
    }

    public List<String> getContent() {
        return content;
    }

    public void setContent(List<String> content) {
        this.content = content;
    }

    public Integer getDocID() {
        return docID;
    }
}
