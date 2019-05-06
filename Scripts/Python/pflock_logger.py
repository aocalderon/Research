import requests
import json
from lxml.html import parse, etree
import pandas as pd
import argparse
import logging
import time

parser = argparse.ArgumentParser()
parser.add_argument("-m", "--master", default = "localhost", help = "The master host...")
parser.add_argument("-t", "--time", default = 2, help = "Time lapse to log...")
args = parser.parse_args()
logging.basicConfig(filename='monitor.log',level=logging.INFO, format='%(asctime)s|%(message)s')
clocktime = lambda: int(round(time.time() * 1000))
timer = lambda x: round((clocktime() - x) / 1000.0, 2)
timeToLog = int(args.time)
isTimeToLog = lambda x: int(x) % timeToLog == 0 
master_host = args.master

def main():
    stageName = ""
    while True:
        try:
            response = requests.get("http://{}:4040/api/v1/applications".format(master_host))
            apps = json.loads(response.text)
            appID = apps[0]["id"]
            start = int(apps[0]["attempts"][0]["startTimeEpoch"])
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            continue
        if(not isTimeToLog(timer(start))):
            continue

        # Executors...
        try:
            response = requests.get("http://{}:4040/api/v1/applications/{}/executors".format(master_host, appID))
            spark = json.loads(response.text)
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            continue
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
            id = spark[i]['id']
            hostPort = spark[i]['hostPort']
            rddBlocks = spark[i]['rddBlocks']
            totalTasks = spark[i]['totalTasks']
            totalDuration   = round((spark[i]['totalDuration'])/(float(cores)*1000.0), 2)
            totalInputBytes = round(spark[i]['totalInputBytes']/(1024.0*1024.0), 2)
            tblock = tblock + rddBlocks
            ttasks = ttasks + totalTasks
            tdurat = tdurat + totalDuration
            tinput = tinput + totalInputBytes
            log = "NODES|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(timer(start), appID, executors, id, hostPort, stageName, rddBlocks, totalTasks, totalDuration, totalInputBytes)
            logging.info(log)
            
        log = "TOTAL|{}|{}|{}|{}|{}|{}|{}|{}".format(timer(start), appID, executors, stageName, tblock, ttasks, tdurat, tinput)
        logging.info(log)
        log = "SCALE|{}|{}|{}|{}|{:.2f}|{:.2f}|{:.2f}|{:.2f}".format(timer(start), appID, executors, stageName, tblock / executors, ttasks / executors, tdurat / executors, tinput / executors)
        logging.info(log)

        # Stages...
        try:
            response = requests.get("http://{}:4040/api/v1/applications/{}/stages".format(master_host, appID))
            stages = json.loads(response.text)
            stageId = 0
            stageName = ""
            for stage in stages:
                status = stage['status']
                if status == 'ACTIVE':
                    stageId  = stage['stageId']
                    response = requests.get("http://{}:4040/api/v1/applications/{}/stages/{}".format(master_host, appID, stageId))
                    tasks = json.loads(response.text)
                    stageName = tasks[0]['name']
            complete, total = parse("http://{}:4040/stages".format(master_host)).getroot().get_element_by_id("activeStage-table").cssselect("div span")[1].text.strip().split("/")
            completeTasks = float(complete)
            totalTasks    = float(total.split(" ")[0])
            url = "http://{}:4040/stages/stage/?id={}&attempt=0&task.sort=Duration&task.desc=true&task.pageSize=25".format(master_host, stageId)
            tasks = pd.read_html(etree.tostring(parse(url).getroot().get_element_by_id("task-table")))[0]
            cols = [2,5,6]
            tasks.drop(tasks.columns[cols],axis=1,inplace=True)
            #tasks = tasks[tasks['Status'] == "RUNNING"]
            for index, row in tasks.iterrows():
                #logging.info(row)
                logging.info("TASKS|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(timer(start), appID, executors, id, hostPort, stageName, row['ID'], row['Locality Level'], row['Launch Time'], row['Duration  ▾'], row['Input Size / Records'], row['Status']))
        except (IndexError, KeyError, OSError, AttributeError, ZeroDivisionError, requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            pass


if __name__== "__main__":
    main()
