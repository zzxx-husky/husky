import bindings.backend.library.functional as functional
# import bindings.backend.library.graph as graph
# import bindings.backend.library.word as word
import bindings.backend.library.linear_regression as LinearR
import bindings.backend.library.logistic_regression as LogisticR
# import bindings.backend.library.spca as SPCA
# import bindings.backend.library.tfidf as tfidf
# import bindings.backend.library.bm25 as BM25
import bindings.backend.library.svm as SVM

def register_func():
    # register
    # functional
    functional.register_all()
    LinearR.register_all()
    # SVM.register_all()
    LogisticR.register_all()
