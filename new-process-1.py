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
temp_queue = {} # operations that we need to do when we become leader (which timed out previously)

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

def handle_prepare(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num):
    global ballot_number
    global leader

    if incoming_seq_num > ballot_number[0]:
        ballot_number[0] = incoming_seq_num
        with lock:
            leader = src_node
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}{' break '}".encode('utf-8'))
        print(f"sent {dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}")

        for temp_ballot_num, temp_op in temp_queue.items():
            network_server.send(f"{dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op}{' break '}".encode('utf-8'))
    elif incoming_seq_num == ballot_number[0] and incoming_pid > ballot_number[1]:
        with lock:
            leader = src_node
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}{' break '}".encode('utf-8'))
        print(f"sent {dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}")
        
        for temp_ballot_num, temp_op in temp_queue.items():
            network_server.send(f"{dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op}{' break '}".encode('utf-8'))

def handle_promise(incoming_op_log):
    # to do: decide how to update the local log with the log recieved from nodes
    global op_log
    global promise_count
    op_log = incoming_op_log
    with lock:
        promise_count += 1

def handle_accept(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, operation):
    global accepted_num
    global accepted_val

    if incoming_op_num < ballot_number[2]:
        network_server.send(f"{dst_node} {src_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}{' break '}".encode('utf-8'))
        print(f"{dst_node} {src_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}")
        
        with lock:
            accepted_num = (incoming_seq_num, incoming_pid, incoming_op_num)
            accepted_val = operation

def handle_accepted():
    global accepted_count
    with lock:
        accepted_count += 1

def handle_decide(operation):
    global accepted_num
    global accepted_val

    with lock:
        ballot_number[2] += 1

    LLM_handler = threading.Thread(target=handle_LLM_query, args=())
    LLM_handler.start()

    with lock:
        op_log.append(operation)
        accepted_num = (0, 0, 0)
        accepted_val = ""

def handle_ack(incoming_seq_num, incoming_pid, incoming_op_num, operation):
    del temp_queue[(incoming_seq_num, incoming_pid, incoming_op_num)]

def select_best_answer(): # check?
    pass

def handle_LLM_query():
    pass

def handle_leader_queue(network_server):
    while True:
        # to do: add to the end of the leader_queue or set equal to temp_queue?
        leader_queue.extend(temp_queue.values())

        while True:
            leader_queue.pop(0)
            network_server.send(f"P1 P2 ACCEPT {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))
            network_server.send(f"P1 P3 ACCEPT {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))

def new_op_to_queue(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, operation):
    if leader == "P1":
        leader_queue.append(operation)
        network_server.send(f"{dst_node} {src_node} ACK {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation} {' break '}".encode('utf-8'))

def start_election(network_server):
    with lock:
        ballot_number[0] += 1
    
    network_server.send(f"P1 P2 PREPARE {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))
    network_server.send(f"P1 P3 PREPARE {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))

    global promise_count
    global accepted_count
    while True:
        if promise_count >=1 :
            global leader
            with lock:
                leader = "P1"
                leader_queue.clear() # clear list of leader operations
                promise_count = 0
                accepted_count = 0
                leader_queue_handler = threading.Thread(target=handle_leader_queue, args=(network_server))
                leader_queue_handler.start()

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
                consensus_op = response_split[2] 
                spliced_op = message

                if consensus_op == "PREPARE": # PREPARE {ballot_num} --> e.g. PREPARE 1 1 2
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    prepare_handler = threading.Thread(target=handle_prepare, args=(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num))
                    prepare_handler.start()

                elif consensus_op == "PROMISE": # PROMISE {ballot_num} {accepted_num} {accepted_val} {op_log[]}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    accepted_seq_id = response_split[6]
                    accepted_pid = response_split[7]
                    accepted_op_num = response_split[8]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_seq_id} {accepted_pid} {accepted_op_num}", "").replace("PROMISE ", "")
                    promise_handler = threading.Thread(target=handle_promise, args=(spliced_op))
                    promise_handler.start()
                
                elif consensus_op == "ACCEPT": # ACCEPT {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "").replace("ACCEPT ", "")
                    accept_handler = threading.Thread(target=handle_accept, args=(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, spliced_op))
                    accept_handler.start()
                
                elif consensus_op == "ACCEPTED": # ACCEPTED {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "").replace("ACCEPTED ", "")
                    accepted_handler = threading.Thread(target=handle_accepted, args=())
                    accepted_handler.start()
                
                elif consensus_op == "DECIDE": # DECIDE {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "").replace("DECIDE ", "")
                    decide_handler = threading.Thread(target=handle_decide, args=(spliced_op,))
                    decide_handler.start()

                elif consensus_op == "ANSWER": # ANSWER {answer} - getting answers from other nodes
                    pass

                elif consensus_op == "NEWOP":
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "").replace("NEWOP ", "")
                    newop_handler = threading.Thread(target=new_op_to_queue, args=(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, spliced_op))
                    newop_handler.start()

                elif consensus_op == "ACK":
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "").replace("ACK ", "")
                    ack_handler = threading.Thread(target=handle_ack, args=(incoming_seq_num, incoming_pid, incoming_op_num, spliced_op))
                    ack_handler.start()

                elif consensus_op == "TIMEOUT":
                    timed_out_op = response_split[3]
                    incoming_seq_num = response_split[4]
                    incoming_pid = response_split[5]
                    incoming_op_num = response_split[6]
                    if timed_out_op == "NEWOP":
                        spliced_op = spliced_op.replace(f"{src_node} {dst_node} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "").replace("NEWOP ", "")
                        newop_election_handler = threading.Thread(target=start_election, args=(network_server,))
                        newop_election_handler.start()

        except:
            break

def handle_user_input(network_server):
    while True:
        operation = input("")
        if leader == "P1":
            with lock:
                leader_queue.append(operation)
        elif leader != "P1" and leader != "": 
            # to do: new thread here?
            network_server.send(f"P1 {leader} NEWOP {operation}{' break '}".encode('utf-8'))
            temp_queue[ballot_number] = operation # until we get ACK
        elif leader == "":
            # to do: store operation in temp_queue?
            election_handler = threading.Thread(target=start_election, args=(network_server,))
            election_handler.start

def start_client():
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s1.bind(('127.0.0.1', 9001)) 
    s1.listen(3)

    network_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    network_server.connect(('127.0.0.1', 9000)) 

    network_server.send(f"P1{' break '}".encode('utf-8'))

    client_handler = threading.Thread(target=handle_server_input, args=(s1, network_server,))
    client_handler.start()

    user_handler = threading.Thread(target=handle_user_input, args=(s1, network_server,))
    user_handler.start()

if __name__ == "__main__":
    start_client()