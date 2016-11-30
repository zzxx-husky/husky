# from bindings.frontend.library.graphreceiver import GraphReceiver
# from bindings.frontend.library.wordreceiver import WordReceiver
from bindings.frontend.library.linear_regression_receiver import LinearRegressionModelReceiver
from bindings.frontend.library.logistic_regression_receiver import LogisticRegressionModelReceiver
# from bindings.frontend.library.spca_receiver import SPCAReceiver
# from bindings.frontend.library.tfidf_receiver import TFIDFReceiver
# from bindings.frontend.library.bm25receiver import BM25Receiver
from bindings.frontend.library.svm_receiver import SVMReceiver

def register(receiver_map):
    # GraphReceiver.register(receiver_map)
    # WordReceiver.register(receiver_map)
    LinearRegressionModelReceiver.register(receiver_map)
    # SVMReceiver.register(receiver_map)
    LogisticRegressionModelReceiver.register(receiver_map)
    # SPCAReceiver.register(receiver_map)
    # TFIDFReceiver.register(receiver_map)
    # BM25Receiver.register(receiver_map)