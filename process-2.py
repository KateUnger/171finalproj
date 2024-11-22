            # <src> <dst> <operation>
            # P1 P3 PREPARE create <contextid>
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