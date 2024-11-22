import socket
import time
import threading
import sys, os
from queue import PriorityQueue

# port_dict[msg_comps[0]].send(f"C1 REPLY {msg_comps[2]} {msg_comps[3]}{' break '}".encode('utf-8')) #todo change depending on client
# other_client.send(f"C1 initial{' break '}".encode('utf-8')) #todo change depending on client


ballot_number = (0, 1, 0) # <seq_num, pid, op_num>
leader = "" # initialize leader to empty string, to be changed once leader has been elected
accepted_num = (0, 0, 0)
accepted_val = ""
op_log = []
leader_queue = []

promise_count = 0
accepted_count = 0

lock = threading.Lock()

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
        case _:
            print("The language doesn't matter, what matters is solving problems.")

def handle_prepare(network_server, src_node, dst_node, incoming_ballot_num): # incoming_ballot_num is in form (x, y, z)
    global ballot_number
    global leader
    incoming_seq_num = incoming_ballot_num[0]
    incoming_pid = incoming_ballot_num[1]

    if incoming_seq_num > ballot_number[0]:
        ballot_number[0] = incoming_seq_num
        leader = src_node
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_ballot_num} {accepted_num} {accepted_val} {op_log}".encode('utf-8'))
        print(f"sent {dst_node} {src_node} PROMISE {incoming_ballot_num} {accepted_num} {accepted_val} {op_log}")
    elif incoming_seq_num == ballot_number[0] and incoming_pid > ballot_number[1]:
        # todo: you can't reset the local pid to a diff pid so what do you do?
        leader = src_node
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_ballot_num} {accepted_num} {accepted_val} {op_log}".encode('utf-8'))
        print(f"sent {dst_node} {src_node} PROMISE {incoming_ballot_num} {accepted_num} {accepted_val} {op_log}")
        # todo: parse op_log when you receive PROMISE and add to your log
    
def handle_promise():
    global promise_count
    with lock:
     promise_count += 1

def handle_accept(network_server, src_node, dst_node, incoming_ballot_num, operation):
    incoming_op_num = incoming_ballot_num[2]

    if incoming_op_num < ballot_number[2]:
        network_server.send(f"{dst_node} {src_node} ACCEPTED {incoming_ballot_num} {operation}".encode('utf-8'))
        print(f"{dst_node} {src_node} ACCEPTED {incoming_ballot_num} {operation}")
        global accepted_num
        global accepted_val
        accepted_num = incoming_ballot_num
        accepted_val = operation
    # todo: add operation to leader's queue

def handle_accepted():
    global accepted_count
    with lock:
        accepted_count += 1

def handle_decide(operation):
    ballot_number[2] += 1
    # todo: execute_op()
    op_log.append(operation)

def start_election(network_server):
    ballot_number[0] += 1
    network_server.send(f"P1 P2 PREPARE {ballot_number}".encode('utf-8'))
    network_server.send(f"P1 P3 PREPARE {ballot_number}".encode('utf-8'))

def handle_server_input(network_server):
    while True:
        try:
            response = network_server.recv(1024).decode('utf-8')
            print(f"recieved {response}")

            response_split = response.split()
            src_node = response_split[0]
            dst_node = response_split[1]
            consensus_op = response_split[2] # prepare, promise, accept, accepted, decide

            if consensus_op == "PREPARE": # PREPARE {ballot_num}
                incoming_ballot_num = response_split[3]
                handle_prepare(network_server, src_node, dst_node, incoming_ballot_num)
            elif consensus_op == "PROMISE": # PROMISE {ballot_num} {previous accepted ballots and operations that are >= our ballot num}
                handle_promise()
            elif consensus_op == "ACCEPT": # ACCEPT {ballot_num} {operation}
                operation = response_split[3]
                handle_accept(network_server, src_node, dst_node, incoming_ballot_num, operation)
            elif consensus_op == "ACCEPTED": # ACCEPTED {ballot_num} {operation}
                handle_accepted()
            elif consensus_op == "DECIDE": # DECIDE {ballot_num} {operation}
                operation = response_split[3]
                handle_decide(operation)
            elif consensus_op == "ANSWER":  # ANSWER {answer} - getting answers from other nodes
                # how to handle getting user input for answer
                pass
            elif consensus_op == "NEWOP":
                operation = response_split[3]
                leader_queue.append(operation)
                network_server.send(f"{dst_node} {src_node} ACK".encode('utf-8'))
            elif consensus_op == "ACK":
                pass # what to do when you get an ack?
            elif consensus_op == "TIMEOUT": # what to do when there is a timeout
                start_election(network_server)

            global promise_count
            global accepted_count
            if promise_count >= 1:
                global leader
                leader = "P1"
                with lock:
                    leader_queue.clear() # clear list of leader operations
                    promise_count = 0
                    accepted_count = 0
                    # todo: update local log of operations with whatever logs have been sent back from other nodes

            if accepted_count >= 1:
                # todo: split string correctly to match with values in leader_queue
                operation = response_split[4] # operation
                op_index = leader_queue.index(operation)
                leader.pop(op_index)
                network_server.send(f"P1 P2 DECIDE {operation}".encode('utf-8'))
                network_server.send(f"P1 P3 DECIDE {operation}".encode('utf-8'))

        except:
            break

def handle_user_input(server_socket, network_server):
    while True:
        operation = input("")
        if leader == "P1": # we are the leader
            leader_queue.append(operation)
        elif leader != "P1":
            network_server.send(f"P1 {leader} NEWOP {operation}".encode('utf-8'))
            # have to check for ack if leader isnt there
        elif leader == "": # start an election
            start_election(network_server) # what happens to the operation when you aren't the leader yet, do we store it somewhere?


def start_client():
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s1.bind(('127.0.0.1', 9001)) 
    s1.listen(3)

    network_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    network_server.connect(('127.0.0.1', 9000)) 

    network_server.send(f"P1{' break '}".encode('utf-8'))

    #Listen to input from network service
    client_handler = threading.Thread(target=handle_server_input, args=(network_server,))
    client_handler.start()

    user_handler = threading.Thread(target=handle_user_input, args=(s1, network_server,))
    user_handler.start()

if __name__ == "__main__":
    #Start connections
    start_client()