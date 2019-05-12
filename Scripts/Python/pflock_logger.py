import requests
import json
from lxml.html import parse, etree
import pandas as pd
import argparse
import logging
import time

parser = argparse.ArgumentParser()
parser.add_argument("-m", "--master", default = "localhost", help = "The master host...")
parser.add_argument("-t", "--time", default = 1, help = "Time lapse to log...")
args = parser.parse_args()
logging.basicConfig(filename='monitor.log',level=logging.INFO, format='%(asctime)s|%(message)s')
clocktime = lambda: int(round(time.time() * 1000))
timer = lambda x: round((clocktime() - x) / 1000.0, 2)
timeToLog = int(args.time)
isTimeToLog = lambda x: int(x) % timeToLog == 0 
master_host = args.master

def main():
    stageID = -1
    stageName = ""
    executors = -1
    executorID = -1
    hostPort = ""
    while True:
        try:
            response = requests.get("http://{}:4040/api/v1/applications".format(master_host))
            apps = json.loads(response.text)
            appID = apps[0]["id"]
            start = int(apps[0]["attempts"][0]["startTimeEpoch"])

            ### Stages...
            
            response = requests.get("http://{}:4040/api/v1/applications/{}/stages".format(master_host, appID))
            stages = json.loads(response.text)
            for stage in stages:
                status = stage['status']
                if status == 'ACTIVE':
                    stageID  = stage['stageId']
                    response = requests.get("http://{}:4040/api/v1/applications/{}/stages/{}".format(master_host, appID, stageID))
                    tasks = json.loads(response.text)
                    stageName = tasks[0]['name']
                    url = "http://{}:4040/stages/stage/?id={}&attempt=0&task.sort=Duration&task.desc=true&task.pageSize=100".format(master_host, stageID)
                    #url = "http://{}:4040/stages/stage/?id={}&attempt=0&task.pageSize=100".format(master_host, stageID)
                    tasks = pd.read_html(etree.tostring(parse(url).getroot().get_element_by_id("task-table")))[0]
                    cols = [2,5,6]
                    tasks.drop(tasks.columns[cols],axis=1,inplace=True)
                    #tasks = tasks[tasks['Status'] == "SUCCESS"]
                    for index, row in tasks.iterrows():
                        #logging.info(row)
                        logging.info("TASKS|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(timer(start), appID, executors, executorID, hostPort, stageID, stageName, row['ID'], row['Locality Level'], row['Launch Time'], row['Duration  ▾'], row['GC Time'], row['Input Size / Records'], row['Status']))
                if status == 'COMPLETE':
                    #stageCompleted = "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(stage['stageId'],stage['name'],stage['numTasks'],stage['executorRunTime'],stage['executorCpuTime'],stage['submissionTime'],stage['completionTime'], stage['inputBytes'],stage['inputRecords'],stage['shuffleReadBytes'],stage['shuffleReadRecords'])
                    #logging.info("COMPLETED|{}|{}|{}|{}|{}".format(timer(start), appID, executors, hostPort, stageCompleted))
                    stageID = stage['stageId']
                    response = requests.get("http://{}:4040/api/v1/applications/{}/stages/{}".format(master_host, appID, stageID))
                    stage = json.loads(response.text)
                    stageName = stage[0]['name']
                    print("{}\t{}\t{}".format(stageID, stageName, len(stage[0]['tasks'])))

            ### Executors...
            
            response = requests.get("http://{}:4040/api/v1/applications/{}/executors".format(master_host, appID))
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
                log = "NODES|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(timer(start), appID, executors, executorID, hostPort, stageID, stageName, rddBlocks, totalTasks, totalDuration, totalInputBytes)
                logging.info(log)
            log = "TOTAL|{}|{}|{}|{}|{}|{}|{}|{}".format(timer(start), appID, executors, stageID, stageName, tblock, ttasks, tdurat, tinput)
            logging.info(log)
            if(executors > 0):
                log = "SCALE|{}|{}|{}|{}|{}|{:.2f}|{:.2f}|{:.2f}|{:.2f}".format(timer(start), appID, executors, stageID, stageName, tblock / executors, ttasks / executors, tdurat / executors, tinput / executors)
                logging.info(log)
            
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError, IndexError, KeyError, OSError, AttributeError, ZeroDivisionError):
            continue

if __name__== "__main__":
    main()
