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
    
#Helper function to set link between two nodes to the value passed in
def set_link(node_to_change, other_node, value):
    global lock
    num_to_change = int(node_to_change.replace("P",""))
    other_num = int(other_node.replace("P",""))
    if num_to_change < other_num:
        with lock:
            links[(node_to_change, other_node)] = value
    elif other_num < num_to_change:
        with lock:
            links[(other_node, node_to_change)] = value

#Helper function to make sure nodes exist 
# (e.g. if P4 is passed in, we should ignore the request)
def is_not_node(node):
    if node == "P1" or node == "P2" or node == "P3":
        return False #node is correct
    else:
        return True #node does not exist or is incorrect

    
#Helper function to check that node is alive
def node_alive(node):
    return node_status[node]

#Helper function to set all of a node's links
def change_node_links(node, value):
    if node == "P1":
            set_link("P1","P2",value)
            set_link("P1","P3",value)
    elif node == "P2":
        set_link("P2","P1",value)
        set_link("P2","P3",value)
    elif node == "P3":
        set_link("P3","P1",value)
        set_link("P3","P2",value)

#Function to close all sockets and exit program
def do_exit():
    #Close all open sockets
    connections["NS"].close()
    if node_alive("P1"):
        connections["P1"].close()
    if node_alive("P2"):
        connections["P2"].close()
    if node_alive("P3"):
        connections["P3"].close()
    sys.stdout.flush()
    os._exit(0)

#Function to execute the failLink Command
#failLink <src> <dst> = Comm link fil between src and dst nodes (no msgs betwn the nodes)
def do_fail_link(message):
    global lock
    message_split = message.split(" ")
    src_node = message_split[1]
    dst_node = message_split[2]
    #Check message for input error
    if is_not_node(src_node) or is_not_node(dst_node):
        print("Incorrect Node ID")
        return
    #Find link in links{}
    # set_link(src_node, dst_node) = False
    set_link(src_node, dst_node, False)
    print(f"links = {links}")


#Function to execute the fixLink Command
#fixLink <src> <dst> = Counter to failLink
def do_fix_link(message):
    global lock
    message_split = message.split(" ")
    src_node = message_split[1]
    dst_node = message_split[2]
    #Check message for input error
    if is_not_node(src_node) or is_not_node(dst_node):
        print("Incorrect Node ID")
        return
    #Find link in links{}
    src_num = int(src_node.replace("P",""))
    dst_num = int(dst_node.replace("P",""))
    set_link(src_num, dst_num, True)

#Function to execute the failNode Command
#failNode <nodeNum> = Kill node (crash failure) and must restart node after
def do_fail_node(message):
    global lock
    message_split = message.split(" ")
    node = message_split[1]
    #Check message for input error
    if is_not_node(node):
        print("Incorrect Node ID")
        return
    if node in node_status:
        with lock:
            node_status[node] = False
        socket = connections[node]
        print(f"        SEND (to {node}) EXIT\n")
        socket.send(f"EXIT{' break '}".encode('utf-8'))
        with lock:
            del connections[node]
        change_node_links(node, False)
        
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
            match operation: 
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
    #Get src and dst nodes (from which node to which node)
    src_node = message_split[0]
    dst_node = message_split[1]
    #Check for input error (e.g. nodes are incorrect)

    if is_not_node(src_node) or is_not_node(dst_node):
        print("Incorrect Node ID")
        return
    #Check that link exists between node
    if link_works(src_node, dst_node) and node_alive(dst_node) and (dst_node in connections):
        #Send message to dst node
        dst_socket = connections[dst_node]
        print("        SEND " + str(message) + "\n")
        dst_socket.send(f"{message}{' break '}".encode('utf-8'))
    else:
        if node_alive(src_node) and (src_node in connections):
            src_socket = connections[src_node]
            #Add TIMEOUT key word after nodes but before operation and send back to src_node
            #e.g. P1 P3 TIMEOUT Prepare ...
            message_reordered = message.replace(f"{src_node} {dst_node}", f"{src_node} {dst_node} TIMEOUT")
            print("        SEND " + str(message_reordered) + "\n")
            src_socket.send(f"{message_reordered}{' break '}".encode('utf-8'))


#Function to receive and process messages from P1, P2, P3
def handle_client(client_socket, addr):
    global lock
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
                # print("NEW MESSAGE " + str(message))

                #todo add 3 second delay 
                #Get src process id and check that it has been logged in connections dictionary
                if (message == "P1") or (message == "P2") or (message == "P3"):
                    with lock:
                        connections[message] = client_socket
                        node_status[message] = True
                    change_node_links(message, True)
                else:
                    #Send message to destination pid
                    do_task_client_thread = threading.Thread(target=send_msg, args=(message,))
                    do_task_client_thread.start() 
        except:
            break

#Function to set-up and handle connections
def start_client():
    global lock
    #Host socket for processes to connect to:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #Add host socket to connections dictionary for easy exit (in do_exit())
    with lock:
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
    lock = threading.Lock()

    with lock:
        links[("P1", "P2")] = False
        links[("P1", "P3")] = False
        links[("P2", "P3")] = False
        #Initialize node_status dictionary
        node_status["P1"] = False
        node_status["P2"] = False
        node_status["P3"] = False
    #Start input 
    input_handler = threading.Thread(target = do_input)
    input_handler.start()
    #Start connections
    start_client()