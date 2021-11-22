import socket
import json
import time


#socket init
HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR)


data = []
#sending req function 
def send(msg):
    message = msg.encode(FORMAT)
    # msg_length = len(message)
    # send_length = str(msg_length).encode(FORMAT)
    # send_length += b' ' * (HEADER - len(send_length))
    # client.send(send_length)
    client.send(message)
    #recieving bacthes
    
    
    
    

def recv_timeout(the_socket,timeout=2):
    #make socket non blocking
    the_socket.setblocking(0)
    
    #total data partwise in an array
    total_data=[]
    data=''
    
    #beginning time
    begin=time.time()
    while 1:
        #if you got some data, then break after timeout
        if total_data and time.time()-begin > timeout:
            break
        
        #if you got no data at all, wait a little longer, twice the timeout
        elif time.time()-begin > timeout*2:
            break
        
        #recv something
        try:
            data = the_socket.recv(8192).decode(FORMAT)
            if data:
                total_data.append(data)
                with open("DataRecieved.json", "a", encoding= "utf-8") as jsonfile:
                        jsonfile.writelines(data)
                #change the beginning time for measurement
                begin=time.time()
            else:
                #sleep for sometime to indicate a gap
                time.sleep(0.1)
        except:
            pass
    
    #join all parts to make final string
    return ''.join(total_data)  

# this is where the body of request can be modified
request = {"ID": "2", "Benchmark_Type": "DVD","Workload_Metric": "Memory", "Data_Type": "Test", "Batch_Unit": "500",
"Batch_ID": "8", "Batch_Size": "3" }

# serializing and sending the requst.
print("[REQUESTING] sending data to server...")
jreq = json.dumps(request)
send(jreq)

#Receieivng RFWID and Last Batch ID 

print("[RECEIVING] receiving data from server...")
rfid = client.recv(10).decode(FORMAT)
print(f"The RFWID is {rfid}")

last_batch = client.recv(10).decode(FORMAT)
print(f"The Last Batch ID sent is {last_batch}")



#call the recieving function
recv_timeout(client)

print("[SAVING} Your data has been stored on DataRecieved.json in Json format...")


# sending disconnect message 
send(DISCONNECT_MESSAGE)

print("[DISCONNECTING] You are now disconnected from the server...")