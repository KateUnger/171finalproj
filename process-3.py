import socket
import time
import threading
import sys, os
from threading import Semaphore
import google.generativeai as genai
import apikey
import ast

model = genai.GenerativeModel("gemini-1.5-flash")
# TODO change when copy and pasting!
pid = 3
ballot_number = (0, 3, 0) # <seq_num, pid, op_num>
leader = "" # initialize leader to empty string, to be changed once leader has been elected
accepted_num = (0, 0, 0)
accepted_val = ""
op_log = []
leader_queue = [] # [ [operation, originating proces], [], .... []]
temp_queue = {} # operations that we need to do when we become leader (which timed out previously), {ballot_number: [operation, originating process]}
contexts = {}
election_in_progress = False

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
    global pid
    # leader = ""

    if int(incoming_seq_num) > ballot_number[0]: # to do: > or >=        
        with lock:
            ballot_number = (int(incoming_seq_num), int(incoming_pid), int(incoming_op_num))
            leader = src_node
            # print("NEW LEADER: ", leader)
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}{' break '}".encode('utf-8'))
        print(f"\nSEND {dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}")

        for temp_ballot_num, temp_op in temp_queue.items():
            network_server.send(f"{dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op[0]}{' break '}".encode('utf-8'))
            # print(f"\nSEND {dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op[0]}")
    
    elif int(incoming_seq_num) == ballot_number[0] and int(incoming_pid) >= ballot_number[1] and int(incoming_op_num) >= ballot_number[2]:
        with lock:
            ballot_number = (int(incoming_seq_num), int(incoming_pid), int(incoming_op_num))
            leader = src_node
            # print("NEW LEADER: ", leader)
        network_server.send(f"{dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}{' break '}".encode('utf-8'))
        print(f"\nSEND {dst_node} {src_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_num[0]} {accepted_num[1]} {accepted_num[2]} {accepted_val} {op_log}")
        
        for temp_ballot_num, temp_op in temp_queue.items():
            network_server.send(f"{dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op[0]}{' break '}".encode('utf-8'))
            # print(f"\nSEND {dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op[0]}")
    
    elif (int(incoming_seq_num) < ballot_number[0]) or (int(incoming_seq_num) == ballot_number[0] and int(incoming_pid) < ballot_number[1]) or (int(incoming_seq_num) == ballot_number[0] and int(incoming_pid) == ballot_number[1] and int(incoming_op_num) < ballot_number[2]):
        #Ballot Number is lower, we are the leader
        # print("HERE")
        network_server.send(f"{dst_node} {src_node} LEADER {leader}{' break '}".encode('utf-8'))

def handle_promise(incoming_op_log, incoming_seq_num, incoming_pid, incoming_op_num):
    # to do: decide how to update the local log with the log recieved from nodes
    global op_log
    global promise_count_map
    incoming_op_log = ast.literal_eval(incoming_op_log)

    if incoming_op_log:
        if int(incoming_seq_num) > int(ballot_number[0]):
            op_log = incoming_op_log
        elif int(incoming_seq_num) == ballot_number[0] and int(incoming_pid) >= ballot_number[1]:
            # print("incoming op_log changed bc of pid")
            op_log = incoming_op_log
        # op_log.extend(incoming_op_log) 
    # print("op_log: ", op_log)
    
    with lock:
        key = int(incoming_seq_num), int(incoming_pid), int(incoming_op_num)
        if key not in promise_count_map: 
          promise_count_map[key] = 0
        promise_count_map[key] += 1

def handle_accept(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, operation):
    global accepted_num
    global accepted_val
    global leader

    with lock:
        if leader == "":
            leader = src_node

    if int(incoming_seq_num) > int(ballot_number[0]):
        network_server.send(f"{dst_node} {src_node} RECOVERY_ASK {' break '}".encode('utf-8'))
    elif int(incoming_seq_num) == int(ballot_number[0]) and int(incoming_op_num) > ballot_number[2] + 1:
        network_server.send(f"{dst_node} {src_node} RECOVERY_ASK {' break '}".encode('utf-8'))
    elif (int(incoming_seq_num) < ballot_number[0]) or (int(incoming_seq_num) == ballot_number[0] and int(incoming_pid) < ballot_number[1]) or (int(incoming_seq_num) == ballot_number[0] and int(incoming_pid) == ballot_number[1] and int(incoming_op_num) < ballot_number[2]):
        network_server.send(f"{dst_node} {src_node} LEADER {leader} {' break '}".encode('utf-8'))
        network_server.send(f"{dst_node} {src_node} RECOVERY_RESP_oplog {op_log}{' break '}".encode('utf-8'))
        network_server.send(f"{dst_node} {src_node} RECOVERY_RESP_contexts {contexts}{' break '}".encode('utf-8'))
    
    if int(incoming_op_num) >= ballot_number[2]:
        network_server.send(f"{dst_node} {src_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}{' break '}".encode('utf-8'))
        print(f"\nSEND {dst_node} {src_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}")
        
        with lock:
            accepted_num = (incoming_seq_num, incoming_pid, incoming_op_num)
            accepted_val = operation

def handle_accepted(incoming_seq_num, incoming_pid, incoming_op_num): 
    global accepted_count_map
    key = (int(incoming_seq_num), int(incoming_pid), int(incoming_op_num))
    # print(f"Handle Accepted: key = {key}, key in map = {key in accepted_count_map}")
    if key not in accepted_count_map: 
        accepted_count_map[key] = 0
    accepted_count_map[key] += 1

def handle_decide(network_server, operation, query_src, incoming_seq_num, incoming_pid, incoming_op_num, src_node):
    global accepted_num
    global accepted_val
    global ballot_number
    global op_log
    global leader

    with lock:
        if leader == "":
            leader = src_node
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
                print(f"\nCREATE NEW CONTEXT {context_id}")
        case "query":
            context_id = operation_split[1]
            if context_id not in contexts:
                print(f"ERROR: Context ID {context_id} does not exist") # to do: delete from context if invalid command
            else:
                query = spliced_op.replace(f"query {context_id} ", "")
                LLM_handler = threading.Thread(target=handle_LLM_query, args=(network_server, query_src, context_id, query, incoming_seq_num, incoming_pid, incoming_op_num))
                LLM_handler.start()
        case "choose":
            context_id = operation_split[1]
            response_num = int(operation_split[2])
            context_data = contexts[context_id]
            context_parts = context_data.split("\nResponse:")
            # print(context_parts)

            foundLastQuery = False
            responsesForLastQuery = []
            updated_context = []
            for i in range(len(context_parts) - 1, -1, -1):
                if "Query:" in context_parts[i]:
                    # context_parts[i] += "\n"
                    foundLastQuery = True
                    updated_context = context_parts[0:i + 1]
                    break
                elif foundLastQuery == False:
                    responsesForLastQuery.append(context_parts[i])
                    del context_parts[i]

            responsesForLastQuery.reverse()

            if not (0 <= response_num - 1 < len(responsesForLastQuery)):
                raise ValueError("Invalid response number!")

            contexts[context_id] = "\nResponse:".join(updated_context + [responsesForLastQuery[response_num - 1]])

    with lock: # to do: does op_log need to have ballot numbers with it?
        op_log.append(operation)
        # print("op_log: ", op_log)
        accepted_num = (0, 0, 0)
        accepted_val = ""

def handle_ack(incoming_seq_num, incoming_pid, incoming_op_num, operation):
    del temp_queue[(int(incoming_seq_num), int(incoming_pid), int(incoming_op_num))]

def handle_answer(context_id, response, incoming_seq_num, incoming_pid, incoming_op_num):
    global answer_count_map
    key = int(incoming_seq_num), int(incoming_pid), int(incoming_op_num)
    if key not in answer_count_map: 
        answer_count_map[key] = 0
    # print(f"answer_count_map: {int(incoming_seq_num)}, {int(incoming_pid)}, {int(incoming_op_num)}")
    answer_count_map[key] += 1
    # print(f"answer count map for {response} : {answer_count_map[key]}")

    contexts[context_id] += f"\nResponse: {response}"

def select_best_answer(network_server, context_id, query_src, response, incoming_seq_num, incoming_pid, incoming_op_num): 
    global answer_count_map
    global leader

    start_time = time.time()

    if leader != "P3":
        network_server.send(f"P3 {leader} ANSWER {incoming_seq_num} {incoming_pid} {incoming_op_num} {context_id} {response}{' break '}".encode('utf-8'))
        # print(f"\nSEND P3 {leader} ANSWER {incoming_seq_num} {incoming_pid} {incoming_op_num} {context_id} {response}")
    elif leader == "P3":
        contexts[context_id] += f"\n\nResponse: {response}"
        this_ballot_num = int(incoming_seq_num), int(incoming_pid), int(incoming_op_num)
        answer_count_map[this_ballot_num] = 0
        while True:
            if answer_count_map[this_ballot_num] == 2:
                response_choices = ""
                # print("Select one of the following responses for your query")
                context_data = contexts[context_id]
                responses = context_data.split("Query:")[-1]
                responses_parts = responses.split("\nResponse:")
                # print(context_data.split("Query:"))
                # print(responses_parts)
                for i, text in enumerate(responses_parts):
                    if i == 0: 
                        response_choices += f"\nQuery: {text}"
                    else:
                        response_choices += f"\nResponse: {text}"
                break

            elif time.time() - start_time > 20:
                # print(f"Timeout reached: proceed without all responses for context {context_id}")
                response_choices = ""
                context_data = contexts[context_id]
                responses = context_data.split("Query:")[-1]
                responses_parts = responses.split("\nResponse:")
                for i, text in enumerate(responses_parts):
                    if i == 0: 
                        response_choices += f"\nQuery: {text}"
                    else:
                        response_choices += f"\nResponse: {text}"
                break

        network_server.send(f"P3 P1 RESPONSES {incoming_seq_num} {incoming_pid} {incoming_op_num} {query_src} {context_id} {response_choices}{' break '}".encode('utf-8'))
        network_server.send(f"P3 P2 RESPONSES {incoming_seq_num} {incoming_pid} {incoming_op_num} {query_src} {context_id} {response_choices}{' break '}".encode('utf-8'))
        # print(f"\nSEND P3 P1 RESPONSES {incoming_seq_num} {incoming_pid} {incoming_op_num} {query_src} {context_id} {response_choices}")
        # print(f"\nSEND P3 P2 RESPONSES {incoming_seq_num} {incoming_pid} {incoming_op_num} {query_src} {context_id} {response_choices}")
        
        if query_src == "P3":
            response_choices = response_choices.split("\nResponse: ")
            # print("query src", query_src)
            print("Select one of the following responses for your query")
            for i, text in enumerate(response_choices):
                if i == 0:
                    print(text)
                else:
                    print(f"Response {i}: {text}")

def handle_LLM_query(network_server, query_src, context_id, query, incoming_seq_num, incoming_pid, incoming_op_num):
    # to do: change so that it takes in the entire context, not just the the current query
    contexts[context_id] += f"\nQuery: {query}"
    response = (model.generate_content(contents=contexts[context_id], generation_config={"temperature": 1.2})).text

    select_answer_handler = threading.Thread(target=select_best_answer, args=(network_server, context_id, query_src, response, incoming_seq_num, incoming_pid, incoming_op_num))
    select_answer_handler.start()

    # print(response.text)
    # print("LLM query: ", query)

def handle_leader_queue(network_server):
        global accepted_count_map
        global pid
        global temp_queue
        global lock
        # to do: add to the end of the leader_queue or set equal to temp_queue?

        with lock:
            leader_queue.extend(temp_queue.values())
            print("HERE1", leader_queue)
            temp_queue = {}

        while True:
            semaphore.acquire()
            if len(leader_queue) == 0:
                continue
            if len(temp_queue) != 0:
                with lock:
                    leader_queue.extend(temp_queue.values())
                    print("HERE2", leader_queue)
                    temp_queue = {}
            leader_op = leader_queue.pop(0)
            operation = leader_op[0]
            query_src = leader_op[1]
            this_ballot_num = (int(ballot_number[0]), int(ballot_number[1]), int(ballot_number[2]))
            network_server.send(f"P3 P1 ACCEPT {int(this_ballot_num[0])} {int(this_ballot_num[1])} {int(this_ballot_num[2])} {operation}{' break '}".encode('utf-8'))
            network_server.send(f"P3 P2 ACCEPT {int(this_ballot_num[0])} {int(this_ballot_num[1])} {int(this_ballot_num[2])} {operation}{' break '}".encode('utf-8'))
            print(f"\nSEND P3 P1 ACCEPT {int(this_ballot_num[0])} {int(this_ballot_num[1])} {int(this_ballot_num[2])} {operation}")
            print(f"\nSEND P3 P2 ACCEPT {int(this_ballot_num[0])} {int(this_ballot_num[1])} {int(this_ballot_num[2])} {operation}")

            accepted_count_map[this_ballot_num] = 0

            while True:
                if accepted_count_map[this_ballot_num] >= 1:
                    network_server.send(f"P3 P1 DECIDE {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
                    network_server.send(f"P3 P2 DECIDE {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
                    print(f"\nSEND P3 P1 DECIDE {strip_ballot_num(ballot_number)} {operation}")
                    print(f"\nSEND P3 P2 DECIDE {strip_ballot_num(ballot_number)} {operation}")

                    stripped_ballot_num = strip_ballot_num(ballot_number).split(" ")
                    incoming_seq_num = stripped_ballot_num[0]
                    incoming_pid = stripped_ballot_num[1]
                    incoming_op_num = stripped_ballot_num[2]

                    decide_handler = threading.Thread(target=handle_decide, args=(network_server, operation, query_src, incoming_seq_num, incoming_pid, incoming_op_num,  f"P{pid}"))
                    decide_handler.start()
                    break

def new_op_to_queue(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, operation):
    global leader
    if leader == "P3":
        leader_queue.append([operation, src_node])
        network_server.send(f"{dst_node} {src_node} ACK {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}{' break '}".encode('utf-8'))
    # elif (leader == "P1") or (leader == "P2"):
    #     network_server.send(f"{dst_node} {leader} FWD {src_node} {leader} NEWOP {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}{' break '}".encode('utf-8'))
    elif leader == "":
        network_server.send(f"{dst_node} {src_node} TIMEOUT NEWOP {incoming_seq_num} {incoming_pid} {incoming_op_num} {operation}{' break '}".encode('utf-8'))
    semaphore.release()

def start_election(network_server):
    # print(f"starting election")
    global ballot_number
    global leader
    global pid
    global election_in_progress

    if election_in_progress == True:
        return
    else:
        election_in_progress = True

    leader = ""

    with lock:
        ballot_number = (int(ballot_number[0] + 1), pid, int(ballot_number[2]))
        this_ballot_num = int(ballot_number[0]), int(ballot_number[1]), int(ballot_number[2])
    
    network_server.send(f"P3 P1 PREPARE {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))
    network_server.send(f"P3 P2 PREPARE {strip_ballot_num(ballot_number)}{' break '}".encode('utf-8'))
    print(f"\nSEND P3 P1 PREPARE {strip_ballot_num(ballot_number)}")
    print(f"\nSEND P3 P2 PREPARE {strip_ballot_num(ballot_number)}")

    global promise_count_map
    promise_count_map[this_ballot_num] = 0
    start_time = time.time()
    while True:
        if promise_count_map[this_ballot_num] >=1 :
            with lock:
                if ((int(this_ballot_num[0]) >= int(ballot_number[0]))
                     and (int(this_ballot_num[1]) >= int(ballot_number[1])) 
                     and (int(this_ballot_num[2]) >= int(ballot_number[2]))):
                    leader = "P3"
                    # print("\nP3 is the leader")
                leader_queue.clear()
                leader_queue_handler = threading.Thread(target=handle_leader_queue, args=(network_server,))
                leader_queue_handler.start()
                election_in_progress = False
                break
        elif time.time() - start_time > 10:
            # print(f"Election timed out, stop election")
            election_in_progress = False
            break

def handle_server_input(s1, network_server):
    global op_log
    global contexts
    while True:
        try:
            stream = network_server.recv(1024).decode('utf-8') 
            messages = stream.split(' break ')
            for message in messages:
                if not message:
                    continue
                time.sleep(3)

                response_split = message.split(" ")
                if response_split[0] == "EXIT":
                    do_exit(s1, network_server)

                src_node = response_split[0]
                dst_node = response_split[1]
                consensus_op = response_split[2] 
                spliced_op = message

                if consensus_op == "FWD": # FWD {orig_src_node} {leader} {fwd_consensus_op} {ballot_num} {operation}
                    final_dest = response_split[4]
                    # print(f"final_dest = {final_dest}")
                    if(final_dest != "P3"):
                        spliced_op = spliced_op.replace(f"{src_node} {dst_node} FWD ", "")
                        # print(f"Spliced_op = {spliced_op}")
                        network_server.send(f"P3 {final_dest} FWD {spliced_op}{' break '}".encode('utf-8'))
                        continue
                    else:
                        spliced_op = spliced_op.replace(f"{src_node} {dst_node} FWD ", "")
                        src_node = response_split[3]
                        dst_node = response_split[4]
                        consensus_op = response_split[5] 
                        response_split = spliced_op.split(" ")

                if consensus_op == "PREPARE": # PREPARE {ballot_num} --> e.g. PREPARE 1 1 2
                    print(f"\nRECIEVED {message}")
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    prepare_handler = threading.Thread(target=handle_prepare, args=(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num))
                    prepare_handler.start()

                elif consensus_op == "PROMISE": # PROMISE {ballot_num} {accepted_num} {accepted_val} {op_log[]}
                    print(f"\nRECIEVED {message}")
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    accepted_seq_id = response_split[6]
                    accepted_pid = response_split[7]
                    accepted_op_num = response_split[8]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} PROMISE {incoming_seq_num} {incoming_pid} {incoming_op_num} {accepted_seq_id} {accepted_pid} {accepted_op_num}", "")
                    promise_handler = threading.Thread(target=handle_promise, args=(spliced_op, incoming_seq_num, incoming_pid, incoming_op_num))
                    promise_handler.start()
                
                elif consensus_op == "ACCEPT": # ACCEPT {ballot_num} {operation} {op_log[]}
                    print(f"\nRECIEVED {message}")
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} ACCEPT {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    accept_handler = threading.Thread(target=handle_accept, args=(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, spliced_op))
                    accept_handler.start()
                
                elif consensus_op == "ACCEPTED": # ACCEPTED {ballot_num} {operation}
                    print(f"\nRECIEVED {message}")
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} ACCEPTED {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    accepted_handler = threading.Thread(target=handle_accepted, args=(incoming_seq_num, incoming_pid, incoming_op_num))
                    accepted_handler.start()
                
                elif consensus_op == "DECIDE": # DECIDE {ballot_num} {operation}
                    print(f"\nRECIEVED {message}")
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} DECIDE {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    decide_handler = threading.Thread(target=handle_decide, args=(network_server, spliced_op, src_node, incoming_seq_num, incoming_pid, incoming_op_num, src_node))
                    decide_handler.start()

                elif consensus_op == "ANSWER": # ANSWER {ballot_num} {context_id} {answer} - getting answers from other nodes, {dst_node} is the originating process
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    context_id = response_split[6]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} ANSWER {incoming_seq_num} {incoming_pid} {incoming_op_num} {context_id} ", "")
                    answer_handler = threading.Thread(target=handle_answer, args=(context_id, spliced_op, incoming_seq_num, incoming_pid, incoming_op_num))
                    answer_handler.start()

                elif consensus_op == "LEADER":
                    global leader
                    global temp_queue
                    leader = response_split[3]
                    for temp_ballot_num, temp_op in temp_queue.items():
                        network_server.send(f"{dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op[0]}{' break '}".encode('utf-8'))
                        # print(f"\nSEND {dst_node} {src_node} NEWOP {temp_ballot_num[0]} {temp_ballot_num[1]} {temp_ballot_num[2]} {temp_op[0]}")
                 
                elif consensus_op == "RESPONSES": # RESPONSES {ballot_num} {context_id} {response_choices}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    query_src = response_split[6]
                    context_id = response_split[7]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} RESPONSES {incoming_seq_num} {incoming_pid} {incoming_op_num} {query_src} {context_id} ", "")
                    spliced_op = spliced_op.split("\nResponse: ")
                    global contexts
                    
                    if query_src == "P3":
                        print("Select one of the following responses for your query")
                        for i, text in enumerate(spliced_op):
                            if i == 0:
                                print(text)
                            else:
                                print(f"Response {i}: {text}")

                    del spliced_op[0]
                    contexts[context_id] += "\n\nResponse:" + "\nResponse:".join(spliced_op)
                    # print("context after recieving response: ", contexts[context_id])
                
                elif consensus_op == "RECOVERY_RESP_oplog": # RECOVERY 
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} RECOVERY_RESP_oplog ", "")
                    op_log = ast.literal_eval(spliced_op)

                elif consensus_op == "RECOVERY_RESP_contexts": # RECOVERY 
                    spliced_contexts = spliced_op.replace(f"{src_node} {dst_node} RECOVERY_RESP_contexts ", "")
                    contexts = ast.literal_eval(spliced_contexts)

                elif consensus_op == "RECOVERY_ASK": # RECOVERY 
                    network_server.send(f"{dst_node} {src_node} RECOVERY_RESP_oplog {op_log}{' break '}".encode('utf-8'))
                    network_server.send(f"{dst_node} {src_node} RECOVERY_RESP_contexts {contexts}{' break '}".encode('utf-8'))

                elif consensus_op == "NEWOP": # NEWOP {ballot_num} {operation}
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} NEWOP {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    newop_handler = threading.Thread(target=new_op_to_queue, args=(network_server, src_node, dst_node, incoming_seq_num, incoming_pid, incoming_op_num, spliced_op))
                    newop_handler.start()

                elif consensus_op == "ACK": # ACK {ballot_num} {operation}
                    print(f"\nRECIEVED {message}")
                    incoming_seq_num = response_split[3]
                    incoming_pid = response_split[4]
                    incoming_op_num = response_split[5]
                    spliced_op = spliced_op.replace(f"{src_node} {dst_node} ACK {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                    ack_handler = threading.Thread(target=handle_ack, args=(incoming_seq_num, incoming_pid, incoming_op_num, spliced_op))
                    ack_handler.start()

                elif consensus_op == "TIMEOUT":
                    print(f"\nRECIEVED {message}")
                    timed_out_op = response_split[3]

                    if timed_out_op == "NEWOP":
                        spliced_op = spliced_op.replace(f"{src_node} {dst_node} TIMEOUT NEWOP {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                        election_handler = threading.Thread(target=start_election, args=(network_server,))
                        election_handler.start()

                    elif response_split[6] != "FWD":
                        incoming_seq_num = response_split[4]
                        incoming_pid = response_split[5]
                        incoming_op_num = response_split[6]

                        if dst_node == leader:
                            spliced_op = spliced_op.replace(f"{src_node} {dst_node} TIMEOUT {timed_out_op} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                            if leader == "P1":
                                network_server.send(f"P3 P2 FWD P3 P1 {timed_out_op} {incoming_seq_num} {incoming_pid} {incoming_op_num} {spliced_op}{' break '}".encode('utf-8'))
                            elif leader == "P2":
                                network_server.send(f"P3 P1 FWD P3 P2 {timed_out_op} {incoming_seq_num} {incoming_pid} {incoming_op_num} {spliced_op}{' break '}".encode('utf-8'))
                        elif dst_node != leader and leader != "P3":
                            if timed_out_op == "NEWOP":
                                spliced_op = spliced_op.replace(f"{src_node} {dst_node} TIMEOUT NEWOP {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                                # to do: do we need to add the newop to the temp queue? --> we have already added to temp queue before, so we don't need to add again
                                election_handler = threading.Thread(target=start_election, args=(network_server,))
                                election_handler.start()
                        elif dst_node != leader and leader == "P3":
                            spliced_op = spliced_op.replace(f"{src_node} {dst_node} TIMEOUT {timed_out_op} {incoming_seq_num} {incoming_pid} {incoming_op_num} ", "")
                            if dst_node == "P1":
                                network_server.send(f"P3 P2 FWD P3 P1 {timed_out_op} {incoming_seq_num} {incoming_pid} {incoming_op_num} {spliced_op}{' break '}".encode('utf-8'))
                            elif dst_node == "P2":
                                network_server.send(f"P3 P1 FWD P3 P2 {timed_out_op} {incoming_seq_num} {incoming_pid} {incoming_op_num} {spliced_op}{' break '}".encode('utf-8'))
    
                    else:
                        # print("Leader cannot be reached via forwarding, starting an election.")
                        election_handler = threading.Thread(target=start_election, args=(network_server,))
                        election_handler.start()
        except:
            break

def handle_user_input(s1, network_server):
    global contexts
    while True:
        operation = input("")
        split_operation = operation.split(" ")

        if split_operation[0] == "exit":
            do_exit(s1, network_server)
        elif split_operation[0] == "leader":
            global leader
            print(f"Leader = {leader}")
            continue
        elif split_operation[0] == "oplog":
            global op_log
            print(f"Operation log = {op_log}")
            continue
        elif split_operation[0] == "tempqueue":
            global temp_queue
            print(f"Temp Queue = {temp_queue}")
            continue
        elif split_operation[0] == "view":
            operation_split = operation.split(" ")
            context_id = operation_split[1]
            print(f"Context {context_id}: \n{contexts[context_id]}")
            continue
        elif split_operation[0] == "viewall":
            operation_split = operation.split(" ")
            for context_id, context in contexts.items():
                print(f"Context {context_id}:\n")
                print(f"Content:\n{context}")
                print("-" * 30)
            continue

        if leader == "P3":
            with lock:
                leader_queue.append([operation, "P3"])
                semaphore.release()
        elif leader != "P3" and leader != "": 
            network_server.send(f"P3 {leader} NEWOP {strip_ballot_num(ballot_number)} {operation}{' break '}".encode('utf-8'))
            print(f"\nSEND P3 {leader} NEWOP {strip_ballot_num(ballot_number)} {operation}")
            temp_queue[ballot_number] = [operation, "P3"]
        elif leader == "":
            # to do: store operation in temp_queue?
            temp_queue[ballot_number] = [operation, "P3"]
            election_handler = threading.Thread(target=start_election, args=(network_server,))
            election_handler.start()

def start_client():
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s1.bind(('127.0.0.1', 9006)) 
    s1.listen(3)

    network_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    network_server.connect(('127.0.0.1', 9000)) 

    network_server.send(f"P3{' break '}".encode('utf-8'))

    client_handler = threading.Thread(target=handle_server_input, args=(s1, network_server,))
    client_handler.start()

    user_handler = threading.Thread(target=handle_user_input, args=(s1, network_server,))
    user_handler.start()

    genai.configure(api_key=apikey.APIKEY)

if __name__ == "__main__":
    start_client()