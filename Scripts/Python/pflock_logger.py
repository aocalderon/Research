import requests
import json
from lxml.html import parse, etree
import pandas as pd
import argparse
import logging
import time
import sys
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("-m", "--master", default = "localhost", help = "The master host...")
parser.add_argument("-p", "--port", default = "4040", help = "The master port...")
parser.add_argument("-t", "--time", default = 1, help = "Time lapse to log...")
parser.add_argument("-n", "--nohup", default = "", help = "The nohup file path...")
parser.add_argument("-i", "--indexstage", default = 1, help = "Index for stage in logs...")
parser.add_argument("-e", "--indexstatus", default = 1, help = "Index for status in logs...")
args = parser.parse_args()
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s|%(message)s')
clocktime = lambda: int(round(time.time() * 1000))
timer = lambda x: round((clocktime() - x) / 1000.0, 2)
timeToLog = int(args.time)
isTimeToLog = lambda x: int(x) % timeToLog == 0 
master_host = args.master
master_port = args.port
nohup = args.nohup
indexStage  = int(args.indexstage)
indexStatus = int(args.indexstatus)

def main():
    stageID    = -1
    stageName  = ""
    executors  = -1
    executorID = -1
    hostPort   = ""
    myStage    = ""
    myInterval = ""
    currentStageId = -1
    while True:
        try:
            response = requests.get("http://{}:{}/api/v1/applications".format(master_host, master_port))
            apps = json.loads(response.text)
            appID = apps[0]["id"]
            start = int(apps[0]["attempts"][0]["startTimeEpoch"])

            ### Stages...
            
            response = requests.get("http://{}:{}/api/v1/applications/{}/stages".format(master_host, master_port, appID))
            stages = json.loads(response.text)
            for stage in stages:
                status = stage['status']
                if status == 'ACTIVE':
                    stageID  = stage['stageId']
                    response = requests.get("http://{}:{}/api/v1/applications/{}/stages/{}".format(master_host, master_port, appID, stageID))
                    tasks = json.loads(response.text)
                    stageName = tasks[0]['name']
                    url = "http://{}:{}/stages/stage/?id={}&attempt=0&task.sort=Duration&task.desc=true&task.pageSize=200".format(master_host, master_port, stageID)
                    #url = "http://{}:{}/stages/stage/?id={}&attempt=0&task.sort=Status&task.pageSize=100".format(master_host, master_port, stageID)
                    tasks = pd.read_html(etree.tostring(parse(url).getroot().get_element_by_id("task-table")))[0]
                    cols = [2,5,6]
                    tasks.drop(tasks.columns[cols],axis=1,inplace=True)
                    for index, row in tasks.iterrows():
                        taskStatus = row['Status']
                        #if taskStatus == 'RUNNING':
                            #logging.info(row)
                            #logging.info("TASKS|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(myStage, timer(start), appID, executors, executorID, hostPort, stageID, stageName, row['ID'], row['Locality Level'], row['Launch Time'], row['Duration  ▾'], row['GC Time'], row['Input Size / Records'], taskStatus))

                if status == 'COMPLETE':
                    stageId = int(stage['stageId'])
                    name = stage['name']
                    partitions = stage['numTasks']
                    if stageId > currentStageId:
                        response = requests.get("http://{}:{}/api/v1/applications/{}/stages/{}/0/taskList".format(master_host, master_port, appID, stageId))
                        tasks = json.loads(response.text)
                        for task in tasks:
                            taskStatus = task['status']
                            if taskStatus == "SUCCESS":
                                taskId = task['taskId']
                                taskLocality = task['taskLocality']
                                taskHost = task['host']
                                taskDuration = task['duration']
                                taskMetrics = task['taskMetrics']
                                times = "{}|{}|{}|{}|{}".format(taskDuration, taskMetrics['executorDeserializeTime'],taskMetrics['executorRunTime'],taskMetrics['jvmGcTime'],taskMetrics['resultSerializationTime'])
                                logging.info("TASK|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(myStage, timer(start), appID, executors, stageId, name, partitions, taskId, taskLocality, taskHost, times))
                        currentStageId = stageId

            ### Executors...
            
            response = requests.get("http://{}:{}/api/v1/applications/{}/executors".format(master_host, master_port, appID))
            spark = json.loads(response.text)
            driver_node = 0
            for node in range(0,len(spark)):
                if spark[node]['id'] == 'driver':
                    driver_node = node
            del spark[driver_node]
            tblock = 0
            ttasks = 0
            tdurat = 0
            tinput = 0
            executors = len(spark)
            for i in range(0, executors):
                cores = spark[i]['totalCores']
                executorID = spark[i]['id']
                hostPort = spark[i]['hostPort']
                rddBlocks = spark[i]['rddBlocks']
                totalTasks = spark[i]['totalTasks']
                totalDuration   = round((spark[i]['totalDuration'])/(float(cores)*1000.0), 2)
                totalInputBytes = round(spark[i]['totalInputBytes']/(1024.0*1024.0), 2)
                tblock = tblock + rddBlocks
                ttasks = ttasks + totalTasks
                tdurat = tdurat + totalDuration
                tinput = tinput + totalInputBytes
                log = "NODES|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(myStage, timer(start), appID, executors, executorID, hostPort, stageID, stageName, rddBlocks, totalTasks, totalDuration, totalInputBytes)
                logging.info(log)
            log = "TOTAL|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(myStage, timer(start), appID, executors, stageID, stageName, tblock, ttasks, tdurat, tinput)
            logging.info(log)
            if(executors > 0):
                log = "SCALE|{}|{}|{}|{}|{}|{}|{:.2f}|{:.2f}|{:.2f}|{:.2f}".format(myStage,timer(start), appID, executors, stageID, stageName, tblock / executors, ttasks / executors, tdurat / executors, tinput / executors)
                logging.info(log)

            ### nohup...
            if(nohup != ""):
                out = subprocess.run(["tail", "-n1", nohup], capture_output=True)
                line = str(out.stdout)
                logging.info(line[2:-3])
                arr  = line.split("|")
                myStatus = arr[indexStatus].strip()
                if myStatus == "START":
                    myStage    = arr[indexStage].strip()
                    #myInterval = arr[indexInterval][0]
                if myStatus == "END":
                    myStage    = "None"
                    #myInterval = "-1"
            
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError, IndexError, KeyError, OSError, AttributeError, ZeroDivisionError):
            continue

if __name__== "__main__":
    main()
