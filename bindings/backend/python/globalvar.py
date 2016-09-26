import zmq
import sys

class OperationParam:
    data_str = "data"
    url_str = "url"
    lambda_str = "lambda"
    list_str = "list_name"

class GlobalVar:
    actiontype = "action"
    transformationtype = "transformation"
    loadtype = "load"
    librarytype = "library"
    data_chunk = dict()
    name_to_func = dict()
    name_to_type = dict()
    name_to_prefunc = dict()
    name_to_postfunc = dict()

class GlobalSocket:
    # pipe_from_cpp
    # pipe_to_cpp
    @staticmethod
    def init_socket(wid, pid, session_id):
        ctx = zmq.Context()
        GlobalSocket.pipe_from_cpp = zmq.Socket(ctx, zmq.PULL)
        GlobalSocket.pipe_from_cpp.bind("ipc://pyhusky-session-"+session_id+"-proc-"+pid+"-"+wid)
        GlobalSocket.pipe_to_cpp = zmq.Socket(ctx, zmq.PUSH)
        GlobalSocket.pipe_to_cpp.connect("ipc://cpphusky-session-"+session_id+"-proc-"+pid+"-"+wid)

    @staticmethod
    def send(content):
        GlobalSocket.pipe_to_cpp.send(content)

    @staticmethod
    def recv():
        return GlobalSocket.pipe_from_cpp.recv()
