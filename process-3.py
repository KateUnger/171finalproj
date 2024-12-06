import socket
import time
import threading
import sys, os
from threading import Semaphore

ballot_number = (0, 3, 0) # <seq_num, pid, op_num>
leader = "" # initialize leader to empty string, to be changed once leader has been elected
accepted_num = (0, 0, 0)
accepted_val = ""
op_log = []
leader_queue = []
temp_queue = {} # operations that we need to do when we become leader (which timed out previously)

promise_count = 0
accepted_count = 0

accepted_condition = threading.Condition()
semaphore = Semaphore(1)
lock = threading.Lock()

def do_exit(s3, network_server):
    network_server.close()
    s3.close()
    sys.stdout.flush()
    os._exit(0)

def strip_ballot_num(incoming_ballot):
    return str(incoming_ballot).replace(",", "").replace("(", "").replace(")", "")

def handle_prepare(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num):
    global ballot_number
    global leader

    if int(incoming_seq_num) >= ballot_number[0]:
        ballot_number = (int(incoming_seq_num), int(ballot_number[1]), int(ballot_number[2]))
       
        with lock:
            leader = src_node
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}{' break '}".encode('utf-8'))
        print(f"\nSENT:\n {dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}")

        for temp_ballot_num, temp_op in temp_queue.items():
            network_server.send(f"{dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op}{' break '}".encode('utf-8'))
            print(f"\nSENT:\n {dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op}")

    elif int(incoming_seq_num) == ballot_number[0] and int(incoming_pid) >= ballot_number[1]:
        with lock:
            leader = src_node
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}{' break '}".encode('utf-8'))
        print(f"\nSENT:\n {dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}")
        
        for temp_ballot_num, temp_op in temp_queue.items():
            network_server.send(f"{dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op}{' break '}".encode('utf-8'))
            print(f"\nSENT:\n {dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op}")

def handle_promise(incoming_op_log):
    # to do: decide how to update the local log with the log recieved from nodes
    global op_log
    global promise_count
    if('[]' not in incoming_op_log):
        op_log.append(incoming_op_log) 
    print("op_log: ", op_log)

    with lock:
        promise_count += 1

def handle_accept(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, operation):
    global accepted_num
    global accepted_val

    if int(incoming_op_num) >= ballot_number[2]:
        network_server.send(f"{dst_node} {src_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}{' break '}".encode('utf-8'))
        print(f"\nSENT:\n {dst_node} {src_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}")
        
        with lock:
            accepted_num = (incoming_seq_num, incoming_pid, incoming_op_num)
            accepted_val = operation

def handle_accepted():
    global accepted_count
    accepted_count += 1
    if (accepted_count == 2):
        accepted_count = 0
    print(f"        incremented accepted_count: {accepted_count}")

def handle_decide(operation):
    global accepted_num
    global accepted_val
    global ballot_number
    global op_log

    with lock:
        ballot_number = (int(ballot_number[0]), int(ballot_number[1]), int(ballot_number[2] + 1))

    LLM_handler = threading.Thread(target=handle_LLM_query, args=())
    LLM_handler.start()

    with lock:
        op_log.append(operation)
        print("op_log: ", op_log)
        accepted_num = (0, 0, 0)
        accepted_val = ""

def handle_ack(incoming_seq_num, incoming_pid, incoming_op_num, operation):
    del temp_queue[(int(incoming_seq_num), int(incoming_pid), int(incoming_op_num))]

def select_best_answer(): # check?
    pass

def handle_LLM_query():
    print("temporarily handling LLM query")

def handle_leader_queue(network_server):
        # to do: add to the end of the leader_queue or set equal to temp_queue?
        leader_queue.extend(temp_queue.values()) # to do: parse temp queue differently because it's a dictionary

        while True:
            semaphore.acquire()
            operation = leader_queue.pop(0)
            network_server.send(f"P3 P1 ACCEPT {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
            network_server.send(f"P3 P2 ACCEPT {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
            print(f"\nSENT:\n P3 P1 ACCEPT {strip_ballot_num(ballot_number)} {operation}")
            print(f"\nSENT:\n P3 P2 ACCEPT {strip_ballot_num(ballot_number)} {operation}")

            global accepted_count
            with lock:
                        accepted_count = 0
            while True:
                if accepted_count >= 1:
                    network_server.send(f"P3 P1 DECIDE {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
                    network_server.send(f"P3 P2 DECIDE {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
                    print(f"\nSENT:\n P3 P1 DECIDE {strip_ballot_num(ballot_number)} {operation}")
                    print(f"\nSENT:\n P3 P2 DECIDE {strip_ballot_num(ballot_number)} {operation}")

                    decide_handler = threading.Thread(target=handle_decide, args=(operation,))
                    decide_handler.start()
                    break

def new_op_to_queue(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, operation):
    if leader == "P3":
        leader_queue.append(operation)
        semaphore.release()
        # network_server.send(f"{dst_node} {src_node} ACK {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation} {' break '}".encode('utf-8'))
        # print(f"\nSENT:\n {dst_node} {src_node} ACK {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}")

def start_election(network_server):
    print(f"starting election")
    global ballot_number

    with lock:
        ballot_number = (int(ballot_number[0] + 1), int(ballot_number[1]), int(ballot_number[2]))
    
    network_server.send(f"P3 P1 PREPARE {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))
    network_server.send(f"P3 P2 PREPARE {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))
    print(f"\nSENT:\n P3 P1 PREPARE {strip_ballot_num(ballot_number)}")
    print(f"\nSENT:\n P3 P2 PREPARE {strip_ballot_num(ballot_number)}")

    global promise_count
    global accepted_count
    while True:
        if promise_count >=1 :
            global leader
            with lock:
                leader = "P3"
                print("\nP3 is the leader")
                leader_queue.clear() # clear list of leader operations
                promise_count = 0
                accepted_count = 0
                leader_queue_handler = threading.Thread(target=handle_leader_queue, args=(network_server,))
                leader_queue_handler.start()
                break

def handle_server_input(s3, network_server):
    while True:
        try:
            stream = network_server.recv(1024).decode('utf-8') 
            messages = stream.split(' break ')
            for message in messages:
                if not message:
                    continue
                print(f"\nNEW MESSAGE:\n {message}")
                time.sleep(3)

                response_split = message.split(" ")
                if response_split[0] == "EXIT":
                    do_exit(s3, network_server)

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
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_seq_id} {accepted_pid} {accepted_op_num}", "")
                    promise_handler = threading.Thread(target=handle_promise, args=(spliced_op,))
                    promise_handler.start()
                
                elif consensus_op == "ACCEPT": # ACCEPT {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} ACCEPT {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    accept_handler = threading.Thread(target=handle_accept, args=(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, spliced_op))
                    accept_handler.start()
                
                elif consensus_op == "ACCEPTED": # ACCEPTED {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    accepted_handler = threading.Thread(target=handle_accepted, args=())
                    accepted_handler.start()
                
                elif consensus_op == "DECIDE": # DECIDE {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} DECIDE {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    decide_handler = threading.Thread(target=handle_decide, args=(spliced_op,))
                    decide_handler.start()

                elif consensus_op == "ANSWER": # ANSWER {answer} - getting answers from other nodes
                    pass

                elif consensus_op == "NEWOP":
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op = spliced_op.replace(f"{src_node} {dst_node} NEWOP {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    newop_handler = threading.Thread(target=new_op_to_queue, args=(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, spliced_op))
                    newop_handler.start()

                elif consensus_op == "ACK":
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} ACK {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    ack_handler = threading.Thread(target=handle_ack, args=(incoming_seq_num, incoming_pid, incoming_op_num, spliced_op))
                    ack_handler.start()

                elif consensus_op == "TIMEOUT":
                    timed_out_op = response_split[3]
                    incoming_seq_num = response_split[4]
                    incoming_pid = response_split[5]
                    incoming_op_num = response_split[6]
                    if timed_out_op == "NEWOP":
                        spliced_op = spliced_op.replace(f"{src_node} {dst_node} NEWOP {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                        newop_election_handler = threading.Thread(target=start_election, args=(network_server,))
                        newop_election_handler.start()

        except:
            break

def handle_user_input(s3, network_server):
    while True:
        operation = input("")

        if operation == "exit":
            do_exit(s3, network_server)

        if leader == "P3":
            with lock:
                leader_queue.append(operation)
                semaphore.release()
        elif leader != "P3" and leader != "": 
            network_server.send(f"P3 {leader} NEWOP {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
            print(f"\nSENT:\n P3 {leader} NEWOP {strip_ballot_num(ballot_number)} {operation}")
            temp_queue[ballot_number] = operation # until we get ACK
        elif leader == "":
            # to do: store operation in temp_queue?
            temp_queue[ballot_number] = operation
            election_handler = threading.Thread(target=start_election, args=(network_server,))
            election_handler.start()

def start_client():
    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s3.bind(('127.0.0.1', 9004)) 
    s3.listen(3)

    network_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    network_server.connect(('127.0.0.1', 9000)) 

    network_server.send(f"P3{' break '}".encode('utf-8'))

    client_handler = threading.Thread(target=handle_server_input, args=(s3, network_server,))
    client_handler.start()

    user_handler = threading.Thread(target=handle_user_input, args=(s3, network_server,))
    user_handler.start()

if __name__ == "__main__":
    start_client()