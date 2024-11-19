


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
