import socket
import time
import threading
import sys, os
from queue import PriorityQueue

ballot_number = (0, 1, 0) # <seq_num, pid, op_num>
leader = "" # initialize leader to empty string, to be changed once leader has been elected
accepted_num = (0, 0, 0)
accepted_val = ""
op_log = []
leader_queue = []
temp_queue = [] # operations that we need to do when we become leader (which timed out previously)

promise_count = 0
accepted_count = 0

lock = threading.Lock()

def do_exit(s1, network_server):
    network_server.close()
    s1.close()
    sys.stdout.flush()
    os._exit(0)

def strip_ballot_num(incoming_ballot):
    return incoming_ballot.replace(",", "").replace("(", "").replace(")", "")

def do_input(message, client_socket):
    #Todo
    return

def create_op(message, client_socket):
    #Todo
    return

def choose_op(message, client_socket):
    #Todo
    return

def query_op(message, client_socket):
    #Todo
    return

def view_op(message, client_socket):
    #Todo
    return

def viewall_op(message, client_socket):
    #Todo
    return

def execute_op(message, client_socket):
    message_split = message.split(" ")
    process = message_split[0]
    operation = message_split[1]
    match operation:
        case "create":
            create_op(message, client_socket)
        case "query":
            query_op(message, client_socket)
        case "choose":
            choose_op(message, client_socket)
        case "view":
            view_op(message, client_socket)
        case "viewall":
            viewall_op(message, client_socket)

def handle_prepare(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num): 
    global ballot_number
    global leader

    if incoming_seq_num > ballot_number[0]:
        ballot_number[0] = incoming_seq_num
        with lock:
            leader = src_node
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}{' break '}".encode('utf-8'))
        print(f"sent {dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}")
    elif incoming_seq_num == ballot_number[0] and incoming_pid > ballot_number[1]:
        with lock:
            leader = src_node
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}{' break '}".encode('utf-8'))
        print(f"sent {dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}")
    # to do: decide which log you get back from the nodes to update your local log to
def handle_promise(incoming_op_log):
    global op_log
    global promise_count
    op_log = incoming_op_log
    with lock:
        promise_count += 1

def handle_accept(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, operation):
    if incoming_op_num < ballot_number[2]:
        network_server.send(f"{dst_node} {src_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}{' break '}".encode('utf-8'))
        print(f"{dst_node} {src_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}")
        global accepted_num
        global accepted_val
        with lock:
            accepted_num = (incoming_seq_num, incoming_pid, incoming_op_num)
            accepted_val = operation
    # todo: add operation to leader's queue

def handle_accepted():
    global accepted_count
    with lock:
        accepted_count += 1

def handle_decide(operation):
    global accepted_num
    global accepted_val
    with lock:
        ballot_number[2] += 1
        # todo: start thread for execute_op() - LLM execution
        op_log.append(operation)
        accepted_num = (0, 0, 0)
        accepted_val = ""

def start_election(network_server):
    # todo: start a new thread to handle election that sends prepares and waits for correct num promises back
    # then, when we become the leader, start a leader thread
    with lock:
        ballot_number[0] += 1
    network_server.send(f"P1 P2 PREPARE {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))
    network_server.send(f"P1 P3 PREPARE {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))

def handle_server_input(s1, network_server):
    while True:
        try:
            stream = network_server.recv(1024).decode('utf-8') 
            messages = stream.split(' break ')
            for message in messages:
                if not message:
                    continue
                print(f"recieved {message}")

                response_split = message.split(" ")
                if response_split[0] == "EXIT":
                    do_exit(s1, network_server)

                src_node = response_split[0]
                dst_node = response_split[1]
                consensus_op = response_split[2] # prepare, promise, accept, accepted, decide
                spliced_op = message

                if consensus_op == "PREPARE": # PREPARE {ballot_num} --> e.g. PREPARE 1 1 2
                    # to do: where to store the operation
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    handle_prepare(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num)
                elif consensus_op == "PROMISE": # PROMISE {ballot_num} {accepted_num} {accepted_val} {op_log[]}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    accepted_seq_id = response_split[6]
                    accepted_pid = response_split[7]
                    accepted_op_num = response_split[8]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_seq_id} {accepted_pid} {accepted_op_num}", "").replace("PROMISE ", "")
                    handle_promise(spliced_op)
                elif consensus_op == "ACCEPT": # ACCEPT {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "").replace("ACCEPT ", "")
                    handle_accept(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, operation)
                elif consensus_op == "ACCEPTED": # ACCEPTED {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "").replace("ACCEPTED ", "")
                    handle_accepted()
                elif consensus_op == "DECIDE": # DECIDE {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    operation = response_split[3]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "").replace("DECIDE ", "")
                    handle_decide(operation)
                elif consensus_op == "ANSWER":  # ANSWER {answer} - getting answers from other nodes
                    # how to handle getting user input for answer
                    pass
                elif consensus_op == "NEWOP":
                    operation = response_split[3]
                    with lock:
                        leader_queue.append(operation)
                    network_server.send(f"{dst_node} {src_node} ACK{' break '}".encode('utf-8'))
                elif consensus_op == "ACK":
                    pass # what to do when you get an ack?
                elif consensus_op == "TIMEOUT": # what to do when there is a timeout
                    timed_out_op = response_split[3]
                    incoming_seq_num = response_split[4]
                    incoming_pid = response_split[5]
                    incoming_op_num = response_split[6]
                    if timed_out_op == "NEWOP":
                        spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "").replace("NEWOP ", "")
                        with lock:
                            temp_queue.append(spliced_op)
                        # if leader != "P1": # only start election if you are not the leader
                        start_election(network_server)

                global promise_count
                global accepted_count
                if promise_count >= 1:
                    global leader
                    with lock:
                        leader = "P1"
                        leader_queue.clear() # clear list of leader operations
                        promise_count = 0
                        accepted_count = 0
                        # thread to get consensus on leader ops 
                        leader_handler = threading.Thread(target=handle_leader_tasks, args=(s1, network_server,)) # todo: close thread
                        leader_handler.start()

                if leader == "P1":
                    if accepted_count >= 1:
                        incoming_seq_num = response_split[3]
                        incoming_pid = response_split[4]
                        incoming_op_num = response_split[5]
                        spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "").replace("ACCEPTED ", "")
                        # todo: split string correctly to match with values in leader_queue
                        op_index = leader_queue.index(spliced_op)
                        with lock:
                            leader_queue.pop(op_index)
                        network_server.send(f"P1 P2 DECIDE {incoming_seq_num} {incoming_pid} {incoming_op_num} {spliced_op}{' break '}".encode('utf-8'))
                        network_server.send(f"P1 P3 DECIDE {incoming_seq_num} {incoming_pid} {incoming_op_num} {spliced_op}{' break '}".encode('utf-8'))
                        # todo: make a thread to talk to LLM

        except:
            break

def handle_user_input(server_socket, network_server):
    while True:
        operation = input("")
        if leader == "P1": # we are the leader
            with lock:
                leader_queue.append(operation)
        elif leader != "P1":
            network_server.send(f"P1 {leader} NEWOP {operation}{' break '}".encode('utf-8'))
            # have to check for ack if leader isnt there
        elif leader == "": # start an election
            start_election(network_server) # what happens to the operation when you aren't the leader yet, do we store it somewhere?

def handle_leader_tasks(s1, network_server):
    # to-do
    pass

def start_client():
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s1.bind(('127.0.0.1', 9001)) 
    s1.listen(3)

    network_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    network_server.connect(('127.0.0.1', 9000)) 

    network_server.send(f"P1{' break '}".encode('utf-8'))

    #Listen to input from network service
    client_handler = threading.Thread(target=handle_server_input, args=(s1, network_server,))
    client_handler.start()

    user_handler = threading.Thread(target=handle_user_input, args=(s1, network_server,))
    user_handler.start()

if __name__ == "__main__":
    #Start connections
    start_client()