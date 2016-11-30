import numpy as np
import bindings.frontend as ph
from bindings.frontend.library.logistic_regression import LogisticRegressionModel

ph.env.pyhusky_start()

def line_parse(line):
    data = line.split()
    return ( np.array(data[:-1], dtype=float), float(data[-1]) )

def logistic_regresion():
    # Load data into PyHuskyList
    train_list = ph.env.load("hdfs:///datasets/regression/MSD/test").map(line_parse)

    # Train the model
    Logistic_model = LogisticRegressionModel()
    Logistic_model.load_pyhlist(train_list)
    Logistic_model.train(n_iter = 10, alpha = 0.1)

    # Show the parameter
    print "Vector of Parameters:"
    print Logistic_model.get_param()
    print "intercpet term: " + str(Logistic_model.get_intercept())

def logistic_regresion_hdfs():
    LogisticR_model = LogisticRegressionModel()
    # Data can be loaded from hdfs directly
    # By providing hdfs url
    LogisticR_model.load_hdfs("hdfs:///datasets/classification/a9t", is_sparse = 1, format = "tsv")
    # Train the model
    LogisticR_model.train(n_iter = 10, alpha = 0.1, is_sparse = 1)

    # Show the parameter
    print "Vector of Parameters:"
    print LogisticR_model.get_param()
    print "intercpet term: " + str(LogisticR_model.get_intercept())

# logistic_regresion_hdfs()
logistic_regresion()