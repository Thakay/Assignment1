import socket 
import threading
import json
import csv

##Socket settings 
HEADER = 64
PORT = 5050
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

#Info predefined
num_of_samples = {"DVDTest": 12751,"DVDTrain": 15671, "NDTest": 1047, "NDTrain": 15654 }

metrics = {"CPU": "CPUUtilization_Average","NetIn": 'NetworkIn_Average', "NetOut": 'NetworkOut_Average'
, "Memory": 'MemoryUtilization_Average'}

# directory of CSV files as database
data_dir = {"DVDTest":r"./Data/DVD-testing.csv","DVDTrain":r"./Data/DVD-testing.csv","NDTest":r"./Data/DVD-testing.csv"
,"NDTrain":r"./Data/DVD-testing.csv"}

#To store everything in batches like jsontest
batches_json = []
RFWID = 0
#function to make batches out of the data based on parameters from client 
def make_batch(batch_unit,batch_id,batch_size,nameS,nameM):
    
    batches = []
    
    metric_batches_json = []
    json_format = {}
    
    with open(data_dir[nameS], 'r') as csv_file:
        
        csv_reader = csv.DictReader(csv_file)
        data_key = 0
        for row in csv_reader:
            json_format[data_key] = row
            data_key += 1
            if data_key % batch_unit == 0:
                batches.append(json_format)
                json_format = {}
            elif data_key == num_of_samples[nameS] :
                batches.append(json_format)
                json_format = {}

        loopid = batch_id
        for i in range(batch_size):
            batches_json.append(json.dumps(batches[loopid], indent=2))
            for j in batches[loopid]:
                json_format[j] = batches[loopid][j][metrics[nameM]] 
            metric_batches_json.append(json.dumps(json_format, indent=2))
            json_format = {} 
            loopid += 1
    
    return metric_batches_json
#sending method 
def send(msg,conn):
    message = msg.encode(FORMAT)
    conn.sendall(message)

#handle client request function
def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")

    connected = True
    while connected:
        
        msg = conn.recv(1024).decode(FORMAT)
        #Calculating RFWID
        global RFWID
        RFWID += 1

        #Looking for Disconnect message
        if msg == DISCONNECT_MESSAGE:
            connected = False
            print(f"[{addr}] {msg}")
                
            break

        print(f"[{addr}] {msg}")
        jsonreq = json.loads(msg)
        #parsing the right CSV file out of the request.
        if jsonreq["Benchmark_Type"] == "DVD" :
            if jsonreq["Data_Type"] == "Test":
                names = "DVDTest"
            else:
                names = "DVDTrain"
        else:
            if jsonreq["Data_Type"] == "Test":
                names = "NDTest"
            else:
                names = "NDTrain"
        #getting the metric name 
        namem = jsonreq["Workload_Metric"]

        json_list=make_batch(int(jsonreq["Batch_Unit"]), int(jsonreq["Batch_ID"]), int(jsonreq["Batch_Size"]),names,namem)
        # Sending Last batch RFWID
        send(f"{str(RFWID):<10}",conn)
        # Calculating and Sending Last batch ID
        last_batch_ID = str((int(jsonreq["Batch_ID"]) + int(jsonreq["Batch_Size"]) - 1) )   
        last_batch_ID = f"{last_batch_ID:<10}"
        send(last_batch_ID,conn)

        for batch in json_list:
            send(batch,conn=conn)
        # send(json_list[0],conn=conn)
        # send(json_list[1],conn=conn) and etc ... 
        
        
    conn.close()
        

def start():
    server.listen()
    print(f"[LISTENING] Server is listening on {SERVER}")
    while True:
        conn, addr = server.accept()
        handle_client(conn=conn,addr=addr)
        #threading is commented you can uncomment to use multi client 
        # thread = threading.Thread(target=handle_client, args=(conn, addr))
        # thread.start()
        # print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")


print("[STARTING] server is starting...")
start()