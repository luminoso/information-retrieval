package pt.ua.deti.ir.Coordinator;

public enum PipelineState {
    FAIL,
    IDLE,
    ReadingCorpus,
    Tokenizing,
    Indexing,
}
