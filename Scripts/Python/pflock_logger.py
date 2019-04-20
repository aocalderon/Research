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
timeToLog = args.time
isTimeToLog = lambda x: int(x) % timeToLog == 0 
master_host = args.master

def main():
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
        except (IndexError, KeyError, OSError, AttributeError, ZeroDivisionError, requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            pass

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
        
        if len(spark) > 0:
            i = 0
            cores = spark[i]['totalCores']
            tasks = spark[i]['activeTasks']
            if tasks > cores:
                tasks = cores
            id = spark[i]['id']
            hostPort = spark[i]['hostPort']
            totalTasks = spark[i]['totalTasks']
            totalDuration   = round((spark[i]['totalDuration'])/(float(cores)*1000.0), 2)
            totalInputBytes = round(spark[i]['totalInputBytes']/(1024.0*1024.0), 2)
            log = "{}|{}|{}|{}|{}|{}|{}".format(appID, id, hostPort, stageName, totalTasks, totalDuration, totalInputBytes)
            logging.info(log)

        if len(spark) > 1:
            i = 1
            cores = spark[i]['totalCores']
            tasks = spark[i]['activeTasks']
            if tasks > cores:
                tasks = cores
            id = spark[i]['id']
            hostPort = spark[i]['hostPort']
            totalTasks = spark[i]['totalTasks']
            totalDuration   = round((spark[i]['totalDuration'])/(float(cores)*1000.0), 2)
            totalInputBytes = round(spark[i]['totalInputBytes']/(1024.0*1024.0), 2)
            log = "{}|{}|{}|{}|{}|{}|{}".format(appID, id, hostPort, stageName, totalTasks, totalDuration, totalInputBytes)
            logging.info(log)

        if len(spark) > 2:
            i = 2
            cores = spark[i]['totalCores']
            tasks = spark[i]['activeTasks']
            if tasks > cores:
                tasks = cores
            id = spark[i]['id']
            hostPort = spark[i]['hostPort']
            totalTasks = spark[i]['totalTasks']
            totalDuration   = round((spark[i]['totalDuration'])/(float(cores)*1000.0), 2)
            totalInputBytes = round(spark[i]['totalInputBytes']/(1024.0*1024.0), 2)
            log = "{}|{}|{}|{}|{}|{}|{}".format(appID, id, hostPort, stageName, totalTasks, totalDuration, totalInputBytes)
            logging.info(log)

if __name__== "__main__":
    main()
