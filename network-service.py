import socket
import time
import threading
import sys, os
from queue import PriorityQueue

#Dictionary to track connections
connections = {}

#Dictionary to track links between nodes
#(Pl, Ph): true  ---> this means that there is a link between Pl and Ph
#                |--> note that the lower pid goes first! (e.g. P1,P3 NOT P3,P1)
links = {}

#Dictionary to track nodes alive states
#True = alive, False = restarting
node_status = {}

leader = "" #initialize leader to empty string, to be changed once leader has been elected

lock = threading.Lock()

#Helper function to check that link exists between two nodes
def link_works(src_node, dst_node):
    src_num = int(src_node.replace("P",""))
    dst_num = int(dst_node.replace("P",""))
    if src_num < dst_num:
        return links[(src_node, dst_node)]
    elif dst_num < src_num:
        return links[(dst_node, src_node)]
    
#Helper function to check that node is alive
def node_alive(node):
    #todo: index node status list
    return True

#Function to close all sockets and exit program
def do_exit():
    #Close all open sockets
    #todo: check which nodes are active in connections dict
    connections["NS"].close()
    connections["P1"].close()
    connections["P2"].close()
    connections["P3"].close()
    sys.stdout.flush()
    os._exit(0)

#Function to execute the failLink Command
#failLink <src> <dst> = Comm link fil between src and dst nodes (no msgs betwn the nodes)
def do_fail_link(message):
    global lock
    message_split = message.split("")
    src_node = message_split[1]
    dst_node = message_split[2]
    #Find link in links{}
    src_num = int(src_node.replace("P",""))
    dst_num = int(dst_node.replace("P",""))
    if src_num < dst_num:
        #Writing shared resource in mult-threaded processes--use locks!
        lock.acquire()
        links[(src_node, dst_node)] = False
        lock.release()
    elif dst_num < src_num:
        #Writing shared resource in mult-threaded processes--use locks!
        lock.acquire()
        links[(dst_node, src_node)] = False
        lock.release()

#Function to execute the fixLink Command
#fixLink <src> <dst> = Counter to failLink
def do_fix_link(message):
    global lock
    message_split = message.split("")
    src_node = message_split[1]
    dst_node = message_split[2]
    #Find link in links{}
    src_num = int(src_node.replace("P",""))
    dst_num = int(dst_node.replace("P",""))
    if src_num < dst_num:
        #Writing shared resource in mult-threaded processes--use locks!
        lock.acquire()
        links[(src_node, dst_node)] = True
        lock.release()
    elif dst_num < src_num:
        #Writing shared resource in mult-threaded processes--use locks!
        lock.acquire()
        links[(dst_node, src_node)] = True
        lock.release()

#Function to execute the failNode Command
#failNode <nodeNum> = Kill node (crash failure) and must restart node after
def do_fail_node(message):
    global lock
    message_split = message.split("")
    node = message_split[1]
    if node in node_status:
        node_status[node] = False
        #todo destroy links and remove from connections{}
        #todo send TIMEOUT message (in send_msg()) and send to destroyed node to exit

#Function to collect and execute operations from terminal
#(Run on separate thread!)
def do_input():
    while True:
        try:
            #Get input from terminal
            terminal_msg = input("")
            operation = terminal_msg.split(" ")[0]
            #If message is "exit" then begin exit process
            #Otherwise, create thread to handle operation
            match terminal_msg: 
                case "exit":
                    do_exit()
                case "failLink": #Comm link fil between src and dst nodes (no msgs betwn the nodes)
                    faillink_handler = threading.Thread(target = do_fail_link, args=(terminal_msg,))
                    faillink_handler.start()
                case "fixLink": #Counter to failLink
                    fixlink_handler = threading.Thread(target = do_fix_link, args=(terminal_msg,))
                    fixlink_handler.start()
                case "failNode": #Kill node (crash failure) and must restart node after
                    failnode_handler = threading.Thread(target = do_fail_node, args=(terminal_msg,))
                    failnode_handler.start()
        except:
            break

#Function to relay message from src node to dst node
def send_msg(message):
    #Message format:
    #<src_node> <dst_node> <message>
    #P1 P3 PREPARE create <contextid>
    message_split = message.split(" ")
    #G'et src and dst nodes (from which node to which node)
    src_node = message_split[0]
    dst_node = message_split[1]
    #Check that link exists between node
    if link_works(src_node, dst_node):
        #Get message without src and dst node
        message_to_send = message.replace(src_node, "").replace(dst_node, "")
        if node_alive(dst_node) and (dst_node in connections):
            #Send message (w/o src/dst) to dst node
            dst_socket = connections[dst_node]
            dst_socket.send(f"{message_to_send}{' break '}".encode('utf-8'))

#Function to receive and process messages from P1, P2, P3
def handle_client(client_socket, addr):
    while True:
        try:            
            #Receive and split up messages from processes
            #   Messages will be in the format: "<pid_src> <pid_dst> <message>"
            #   e.g. P1 P3 PREPARE create <contextid>
            stream = client_socket.recv(1024).decode('utf-8') 
            messages = stream.split(' break ')
            for message in messages:
                if not message:
                    continue

                #Get src process id and check that it has been logged in connections dictionary
                if (message == "P1") or (message == "P2") or (message == "P3"):
                    connections[message] = client_socket
                    #Todo fix link, set node to alive
                else:
                    #Send message to destination pid
                    print("starting thread and sending message")
                    do_task_client_thread = threading.Thread(target=send_msg, args=(message,))
                    do_task_client_thread.start() 
        except:
            break

#Function to set-up and handle connections
def start_client():
    #Host socket for processes to connect to:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #Add host socket to connections dictionary for easy exit (in do_exit())
    connections["NS"] = server
    server.bind((socket.gethostbyname(""), 9000))
    server.listen(3) #Listen for P1, P2, P3

    #Handle connections
    while True:
        try:
            client_socket, addr = server.accept()
            client_handler = threading.Thread(target=handle_client, args=(client_socket, addr)) 
            client_handler.start()
        except:
            break



if __name__ == "__main__":
    #Initialize links dictionary
    links[("P1", "P2")] = True
    links[("P1", "P3")] = True
    links[("P2", "P3")] = True
    #Initialize node_status dictionary #todo set to false before they connect
    node_status["P1"] = True
    node_status["P2"] = True
    node_status["P3"] = True
    #Start input 
    input_handler = threading.Thread(target = do_input)
    input_handler.start()
    #Start connections
    start_client()