import socket
import time
import threading
import sys, os
from queue import PriorityQueue

# port_dict[msg_comps[0]].send(f"C1 REPLY {msg_comps[2]} {msg_comps[3]}{' break '}".encode('utf-8')) #todo change depending on client
# other_client.send(f"C1 initial{' break '}".encode('utf-8')) #todo change depending on client



ballot_number = ()
leader = "" #initialize leader to empty string, to be changed once leader has been elected

promise_count = 0
accepted_count = 0

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

def handle_server_input(network_server):
    while True:
        try:
            response = network_server.recv(1024).decode('utf-8')

            # <src> <dst> <message> <operation>
            # message can be: PREPARE (proposer), PROMISE (acceptor), ACCEPT (proposer), ACCEPTED (acceptor), DECIDE
            # if leader is not empty: send message to NS with <dst> leader
            # elif leader empty or response = TIMEOUT: 
                # *start leader election*
                # send PREPARE to NS to <dst> all nodes
            
            # if response = PROMISE
            # promise_count += 1
            # if response = ACCEPTED
            # accepted_count += 1
            # if response = PREPARE
                # if given op_num > local op_num
                # NS send to <dst> PROMISE
                # set leader to <src>
                # else: no reply
            # if repsonse = ACCEPT
                # if given op_num > local op_num
                # NS send to <dst> PROMISE
                # else: no reply
            # if response = DECIDE 
                # do operation

            # if promise_count > majority:
            # leader is decided

            # if accepted_count > majority:
                # get operation from queue
                # send DECIDE to NS to <dst> all nodes
            
            # get user input for answer, then get consensus from all nodes


        except:
            break

def start_client():
    network_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    network_server.connect(('127.0.0.1', 9000)) 

    network_server.send(f"P1{' break '}".encode('utf-8'))

    #Listen to input from network service
    client_handler = threading.Thread(target=handle_server_input, args=(network_server,))
    client_handler.start()

if __name__ == "__main__":
    #Start connections
    start_client()