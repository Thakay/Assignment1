import json
import csv

num_of_samples = {"DVDTest": 12751,"DVDTrain": 15671, "NDTest": 1047, "NDTrain": 15654 }
metrics = {"CPU": "CPUUtilization_Average","NetIn": 'NetworkIn_Average', "NetOut": 'NetworkOut_Average'
, "Memory": 'MemoryUtilization_Average'}
name = "DVDTest"
batch_unit = 1000
num_of_batches = (num_of_samples[name] // batch_unit) + 1
batch_id = 5
batch_size = 3
data_dir = {"DVDTest":r"./Data/DVD-testing.csv","DVDTrain":r"./Data/DVD-testing.csv","NDTest":r"./Data/DVD-testing.csv"
,"NDTrain":r"./Data/DVD-testing.csv"}

batches = []
batches_json = []
metric_batches_json = []
json_format = {}
with open(data_dir["DVDTest"], 'r') as csv_file:
    
    csv_reader = csv.DictReader(csv_file)
    data_key = 0
    for row in csv_reader:
        json_format[data_key] = row
        data_key += 1
        if data_key % batch_unit == 0:
            batches.append(json_format)
            json_format = {}
        elif data_key == num_of_samples[name] :
             batches.append(json_format)
             json_format = {}


    for i in range(batch_size):
        batches_json.append(json.dumps(batches[batch_id], indent=2))
        for j in batches[batch_id]:
            json_format[j] = batches[batch_id][j][metrics["Memory"]] 
        metric_batches_json.append(json.dumps(json_format, indent=2))
        json_format = {} 
        batch_id += 1

    # print(batches_json)
       
    # batches_json.append(json.dumps(batches[12], indent=2))
    with open("jsontestMem.json", "a", encoding= "utf-8") as jsonfile:
        jsonfile.write(batches_json[2])  
    # with open("jsontest2.json", "w", encoding= "utf-8") as jsonfile:
    #     jsonfile.write(batches_json[1])
    
    # print((json.loads(batches_json[0])["2061"]["CPUUtilization_Average"]))
    


