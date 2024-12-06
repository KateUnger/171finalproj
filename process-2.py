import socket
import time
import threading
import sys, os
from threading import Semaphore
import google.generativeai as genai
import apikey

model = genai.GenerativeModel("gemini-1.5-flash")

##TODO change when copy and pasting!
ballot_number = (0, 2, 0) # <seq_num, pid, op_num>
leader = "" # initialize leader to empty string, to be changed once leader has been elected
accepted_num = (0, 0, 0)
accepted_val = ""
op_log = []
leader_queue = [] # [ [operation, originating proces], [], .... []]
temp_queue = {} # operations that we need to do when we become leader (which timed out previously), {ballot_number: [operation, originating process]}
contexts = {}

promise_count_map = {}
accepted_count_map = {}
answer_count_map = {}

accepted_condition = threading.Condition()
semaphore = Semaphore(1)
lock = threading.Lock()

def do_exit(s1, network_server):
    network_server.close()
    s1.close()
    sys.stdout.flush()
    os._exit(0)

def strip_ballot_num(incoming_ballot):
    return str(incoming_ballot).replace(",", "").replace("(", "").replace(")", "")

def handle_prepare(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num):
    global ballot_number
    global leader
    leader = ""

    if int(incoming_seq_num) > ballot_number[0]: # to do: > or >=
        ballot_number = (int(incoming_seq_num), int(ballot_number[1]), ballot_number[2])
        
        with lock:
            leader = src_node
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}{' break '}".encode('utf-8'))
        print(f"\nSENT:\n {dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}")

        for temp_ballot_num, temp_op in temp_queue.items():
            network_server.send(f"{dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op[0]}{' break '}".encode('utf-8'))
            print(f"\nSENT:\n {dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op[0]}")
    
    elif int(incoming_seq_num) == ballot_number[0] and int(incoming_pid) >= ballot_number[1]:
        with lock:
            leader = src_node
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}{' break '}".encode('utf-8'))
        print(f"\nSENT:\n {dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}")
        
        for temp_ballot_num, temp_op in temp_queue.items():
            network_server.send(f"{dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op[0]}{' break '}".encode('utf-8'))
            print(f"\nSENT:\n {dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op[0]}")
    
    elif (leader == ""):
        election_handler = threading.Thread(target=start_election, args=(network_server,))
        election_handler.start()

def handle_promise(incoming_op_log, ballot_number):
    # to do: decide how to update the local log with the log recieved from nodes
    global op_log
    global promise_count_map
    if('[]' not in incoming_op_log):
        op_log.append(incoming_op_log) 
    print("op_log: ", op_log)
    
    with lock:
        promise_count_map[int(ballot_number[0]), int(ballot_number[1]), int(ballot_number[2])] += 1

def handle_accept(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, operation):
    global accepted_num
    global accepted_val

    if int(incoming_op_num) >= ballot_number[2]:
        network_server.send(f"{dst_node} {src_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}{' break '}".encode('utf-8'))
        print(f"\nSENT:\n {dst_node} {src_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}")
        
        with lock:
            accepted_num = (incoming_seq_num, incoming_pid, incoming_op_num)
            accepted_val = operation

def handle_accepted(incoming_seq_num, incoming_pid, incoming_op_num): 
    global accepted_count_map
    accepted_count_map[int(incoming_seq_num), int(incoming_pid), int(incoming_op_num)] += 1

def handle_decide(network_server, operation, query_src):
    print("originating node: ", query_src)
    print("in handle_decide")
    global accepted_num
    global accepted_val
    global ballot_number
    global op_log

    with lock:
        ballot_number = (int(ballot_number[0]), int(ballot_number[1]), int(ballot_number[2] + 1))

    operation_split = operation.split(" ")
    command = operation_split[0]
    spliced_op = operation

    match command:
        case "create":
            context_id = operation_split[1]
            if context_id in contexts:
                print(f"ERROR: Context ID {context_id} already exists")
            else:
                contexts[context_id] = ""
        case "query":
            context_id = operation_split[1]
            if context_id not in contexts:
                print(f"ERROR: Context ID {context_id} does not exist") # TODO delete from context if invalid command
            else:
                query = spliced_op.replace(f"query {context_id} ", "")
                print(f"sending {query} to LLM")
                LLM_handler = threading.Thread(target=handle_LLM_query, args=(network_server, query_src, context_id, query,ballot_number))
                LLM_handler.start()
        case "choose":
            context_id = operation_split[1]
            response = operation_split[2]
            print(f"Got command to choose a response for {context_id}. The chosen response was: {response}")
        case "view":
            context_id = operation_split[1]
            print(f"Context {context_id}: \n{contexts[context_id]}")
        case "viewall":
            for context_id, context in contexts.items():
                print(f"Context {context_id}:\n")
                print(f"Content:\n{context}")
                print("-" * 30)

    with lock: # to do: does op_log need to have ballot numbers with it?
        op_log.append(operation)
        print("op_log: ", op_log)
        accepted_num = (0, 0, 0)
        accepted_val = ""

def handle_ack(incoming_seq_num, incoming_pid, incoming_op_num, operation):
    del temp_queue[(int(incoming_seq_num), int(incoming_pid), int(incoming_op_num))]

def handle_answer(context_id, response, ballot_number):
    global answer_count_map
    answer_count_map[int(ballot_number[0]), int(ballot_number[1]), int(ballot_number[2])] += 1
    # append response to the context

def select_best_answer(network_server, context_id, query_src, response, ballot_number): 
    print("selecting best answer")
    global answer_count_map

    start_time = time.time()
    if query_src == "P2":
        contexts[context_id] += f"\nResponse: {response}"
        answer_count_map[int(ballot_number[0]), int(ballot_number[1]), int(ballot_number[2])] = 0
        while True:
            if answer_count_map[int(ballot_number[0]), int(ballot_number[1]), int(ballot_number[2])] == 2:
                print("Select one of the following responses for your query:")
                print(contexts[context_id])
                print("*********")
                context_data = contexts[context_id]

                lines = context_data.split("\n")
                responses = []
                query_found = False

                for line in reversed(lines):
                    if line.startswith("Response"):
                        responses.append(line)
                    elif not query_found:
                        query_found = True
                    else:
                        break

                if not responses:
                    print(f"No responses found for context ID: {context_id}")
                    return
                
                for response in reversed(responses):
                    print(f"{response}")
                break
            elif time.time() - start_time > 10:
                print(f"Timeout reached: proceed without all responses for context {context_id}")
                break
    else:
        network_server.send(f"P2 {query_src} ANSWER {context_id} {response}{' break '}".encode('utf-8'))
        print("send our LLM answer to the query originating process")
        print(f"\nSENT:\n P2 {query_src} ANSWER {context_id} {response}")

def handle_LLM_query(network_server, query_src, context_id, query, ballot_number):
    print("handling LLM query")
    # to do: change so that it takes in the entire context, not just the the current query
    contexts[context_id] += f"\nQuery: {query}"
    response = (model.generate_content(contents=query, generation_config={"temperature": 1.2})).text
    print(f"LLM response for \"{query}\": {response}")

    select_answer_handler = threading.Thread(target=select_best_answer, args=(network_server, context_id, query_src, response,ballot_number))
    select_answer_handler.start()

    # print(response.text)
    # print("LLM query: ", query)

def handle_leader_queue(network_server):
        global accepted_count_map
        # to do: add to the end of the leader_queue or set equal to temp_queue?
        leader_queue.extend(temp_queue.values())

        while True:
            semaphore.acquire()
            if len(leader_queue) == 0:
                continue
            leader_op = leader_queue.pop(0)
            operation = leader_op[0]
            query_src = leader_op[1]
            network_server.send(f"P2 P1 ACCEPT {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
            network_server.send(f"P2 P3 ACCEPT {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
            print(f"\nSENT:\n P2 P1 ACCEPT {strip_ballot_num(ballot_number)} {operation}")
            print(f"\nSENT:\n P2 P3 ACCEPT {strip_ballot_num(ballot_number)} {operation}")

            accepted_count_map[int(ballot_number[0]),int(ballot_number[1]),int(ballot_number[2])] = 0
            while True:
                if accepted_count_map[int(ballot_number[0]),int(ballot_number[1]),int(ballot_number[2])] >= 1:
                    network_server.send(f"P2 P1 DECIDE {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
                    network_server.send(f"P2 P3 DECIDE {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
                    print(f"\nSENT:\n P2 P1 DECIDE {strip_ballot_num(ballot_number)} {operation}")
                    print(f"\nSENT:\n P2 P3 DECIDE {strip_ballot_num(ballot_number)} {operation}")

                    decide_handler = threading.Thread(target=handle_decide, args=(network_server, operation, query_src))
                    decide_handler.start()
                    break

def new_op_to_queue(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, operation):
    if leader == "P2":
        leader_queue.append([operation, src_node])
        network_server.send(f"{dst_node} {src_node} ACK {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}{' break '}".encode('utf-8'))
    elif leader == "":
        election_handler = threading.Thread(target=start_election, args=(network_server,))
        election_handler.start()
    semaphore.release()
        # print(f"\nSENT:\n {dst_node} {src_node} ACK {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}")

def start_election(network_server):
    print(f"starting election")
    global ballot_number

    with lock:
        ballot_number = (int(ballot_number[0] + 1), int(ballot_number[1]), int(ballot_number[2]))
    
    network_server.send(f"P2 P1 PREPARE {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))
    network_server.send(f"P2 P3 PREPARE {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))
    print(f"\nSENT:\n P2 P1 PREPARE {strip_ballot_num(ballot_number)}")
    print(f"\nSENT:\n P2 P3 PREPARE {strip_ballot_num(ballot_number)}")

    global promise_count_map
    promise_count_map[int(ballot_number[0]), int(ballot_number[1]), int(ballot_number[2])] = 0
    while True:
        if promise_count_map[int(ballot_number[0]), int(ballot_number[1]), int(ballot_number[2])] >=1 :
            global leader
            with lock:
                leader = "P2"
                print("\nP2 is the leader")
                leader_queue.clear()
                leader_queue_handler = threading.Thread(target=handle_leader_queue, args=(network_server,))
                leader_queue_handler.start()
                break

def handle_server_input(s1, network_server):
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
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_seq_id} {accepted_pid} {accepted_op_num}", "")
                    promise_handler = threading.Thread(target=handle_promise, args=(spliced_op,(incoming_seq_num, incoming_pid, incoming_op_num)))
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
                    accepted_handler = threading.Thread(target=handle_accepted, args=(incoming_seq_num, incoming_pid, incoming_op_num))
                    accepted_handler.start()
                
                elif consensus_op == "DECIDE": # DECIDE {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} DECIDE {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    decide_handler = threading.Thread(target=handle_decide, args=(network_server, spliced_op, src_node))
                    decide_handler.start()

                elif consensus_op == "ANSWER": # ANSWER {context_id} {answer} - getting answers from other nodes, {dst_node} is the originating process
                    context_id = response_split[3]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} ANSWER {context_id} ", "")
                    answer_handler = threading.Thread(target=handle_answer, args=(context_id, spliced_op, (incoming_seq_num, incoming_pid, incoming_op_num)))
                    answer_handler.start()

                elif consensus_op == "NEWOP": # NEWOP {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} NEWOP {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    newop_handler = threading.Thread(target=new_op_to_queue, args=(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, spliced_op))
                    newop_handler.start()

                elif consensus_op == "ACK": # ACK {ballot_num} {operation}
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
                        spliced_op = spliced_op.replace(f"{src_node} {dst_node} TIMEOUT NEWOP {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                        # to do: do we need to add the newop to the temp queue? --> we have already added to temp queue before, so we don't need to add again
                        election_handler = threading.Thread(target=start_election, args=(network_server,))
                        election_handler.start()
                    elif (leader == dst_node):
                        election_handler = threading.Thread(target=start_election, args=(network_server,))
                        election_handler.start()

        except:
            break

def handle_user_input(s1, network_server):
    while True:
        operation = input("")

        if operation == "exit":
            do_exit(s1, network_server)

        if leader == "P2":
            with lock:
                leader_queue.append([operation, "P2"])
                semaphore.release()
        elif leader != "P2" and leader != "": 
            network_server.send(f"P2 {leader} NEWOP {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
            print(f"\nSENT:\n P2 {leader} NEWOP {strip_ballot_num(ballot_number)} {operation}")
            # temp_queue[ballot_number] = operation # until we get ACK
            temp_queue[ballot_number] = [operation, "P2"]
        elif leader == "":
            # to do: store operation in temp_queue?
            # temp_queue[ballot_number] = operation
            temp_queue[ballot_number] = [operation, "P2"]
            election_handler = threading.Thread(target=start_election, args=(network_server,))
            election_handler.start()

def start_client():
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s1.bind(('127.0.0.1', 9002)) 
    s1.listen(3)

    network_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    network_server.connect(('127.0.0.1', 9000)) 

    network_server.send(f"P2{' break '}".encode('utf-8'))

    client_handler = threading.Thread(target=handle_server_input, args=(s1, network_server,))
    client_handler.start()

    user_handler = threading.Thread(target=handle_user_input, args=(s1, network_server,))
    user_handler.start()

    genai.configure(api_key=apikey.APIKEY)

if __name__ == "__main__":
    start_client()