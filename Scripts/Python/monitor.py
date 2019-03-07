import curses
from curses import wrapper
import time
from asciichartpy import plot
import random
import requests
import json
from lxml.html import parse, etree
from console_progressbar import ProgressBar
import pandas as pd

def main(stdscr):
    # Make stdscr.getch non-blocking
    master_host = "169.235.25.66"
    stdscr.nodelay(True)
    stdscr.clear()
    width = 125
    series0 = [0] * width
    series0.append(1)
    series1 = [0] * width
    series1.append(1)
    series2 = [0] * width
    series2.append(1)
    gap = 7
    c = 0
    while c != ord('q'):
        try:
            response = requests.get("http://{}:4040/api/v1/applications".format(master_host))
            apps = json.loads(response.text)
            appID = apps[0]["id"]
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            continue
        c = stdscr.getch()
        # Plot some data...
        try:
            response = requests.get("http://{}:4040/api/v1/applications/{}/executors".format(master_host, appID))
            spark = json.loads(response.text)
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            continue
        # Clear out anything else the user has typed in
        curses.flushinp()
        stdscr.clear()

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
            series0.append(tasks)
            if len(set(series0)) == 1:
                value = set(series0).pop()
                if value == cores:
                    value = value - 1
                else:
                    value = value + 1
                series0.append(value)
            stdscr.addstr(0*gap+1, 0, plot(series0))
            stdscr.addstr(0*gap+1 + max(series0) - tasks, width + 11, str(tasks))
            id = spark[i]['id']
            hostPort = spark[i]['hostPort']
            totalTasks = spark[i]['totalTasks']
            totalDuration   = round((spark[i]['totalDuration'])/(float(cores)*1000.0), 2)
            totalInputBytes = round(spark[i]['totalInputBytes']/(1024.0*1024.0), 2)
            stdscr.addstr(0*gap, 0, "Executor {} at {}\tTasks completed: {}\t Duration: {}s\tInput:{}MB".format(id, hostPort, totalTasks, totalDuration, totalInputBytes))
            series0.reverse()
            series0 = series0[0:width]
            series0.reverse()

        if len(spark) > 1:
            i = 1
            cores = spark[i]['totalCores']
            tasks = spark[i]['activeTasks']
            if tasks > cores:
                tasks = cores
            series1.append(tasks)
            if len(set(series1)) == 1:
                value = set(series1).pop()
                if value == cores:
                    value = value - 1
                else:
                    value = value + 1
                series1.append(value)
            id = spark[i]['id']
            hostPort = spark[i]['hostPort']
            totalTasks = spark[i]['totalTasks']
            totalDuration   = round((spark[i]['totalDuration'])/(float(cores)*1000.0), 2)
            totalInputBytes = round(spark[i]['totalInputBytes']/(1024.0*1024.0), 2)
            stdscr.addstr(1*gap, 0, "Executor {} at {}\tTasks completed: {}\t Duration: {}s\tInput:{}MB".format(id, hostPort, totalTasks, totalDuration, totalInputBytes))
            stdscr.addstr(gap+1, 0, plot(series1))
            stdscr.addstr(gap+1 + max(series1) - tasks, width + 11, str(tasks))
            series1.reverse()
            series1 = series1[0:width]
            series1.reverse()

        if len(spark) > 2:
            i = 2
            cores = spark[i]['totalCores']
            tasks = spark[i]['activeTasks']
            if tasks > cores:
                tasks = cores
            series2.append(tasks)
            if len(set(series2)) == 1:
                value = set(series2).pop()
                if value == cores:
                    value = value - 1
                else:
                    value = value + 1
                series2.append(value)
            id = spark[i]['id']
            hostPort = spark[i]['hostPort']
            totalTasks = spark[i]['totalTasks']
            totalDuration   = round((spark[i]['totalDuration'])/(float(cores)*1000.0), 2)
            totalInputBytes = round(spark[i]['totalInputBytes']/(1024.0*1024.0), 2)
            stdscr.addstr(2*gap, 0, "Executor {} at {}\tTasks completed: {}\t Duration: {}s\tInput:{}MB".format(id, hostPort, totalTasks, totalDuration, totalInputBytes))
            stdscr.addstr(2*gap+1, 0, plot(series2))
            stdscr.addstr(2*gap+1 + max(series2) - tasks, width + 11, str(tasks))
            series2.reverse()
            series2 = series2[0:width]
            series2.reverse()
    
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
            #stagesBar = ProgressBar(total=100, prefix='Stages', suffix="{}/{}".format(complete, len(stages)), length=40, fill='#', zfill=' ')
            #stdscr.addstr(3*gap+1, 0, stagesBar.generate_pbar((complete*100.0/len(stages))))
            complete, total = parse("http://{}:4040/stages".format(master_host)).getroot().get_element_by_id("activeStage-table").cssselect("div span")[1].text.strip().split("/")
            completeTasks = float(complete)
            totalTasks    = float(total.split(" ")[0])
            tasksBar = ProgressBar(total=100, prefix='Tasks ', suffix="{}/{}".format(complete, total), length=40, fill='#', zfill=' ')
            stdscr.addstr(3*gap+3, 0, "[{}] {}".format(stageId, stageName))
            stdscr.addstr(3*gap+4, 0, tasksBar.generate_pbar((completeTasks*100.0)/totalTasks))
            url = "http://{}:4040/stages/stage/?id={}&attempt=0&task.sort=Duration&task.desc=true&task.pageSize=10".format(master_host, stageId)
            if c == ord('n'):
                url = "http://{}:4040/stages/stage/?id={}&attempt=0&task.sort=ID&task.desc=true&task.pageSize=20".format(master_host, stageId)
            tasks = pd.read_html(etree.tostring(parse(url).getroot().get_element_by_id("task-table")))[0]
            cols = [0,2,5,6]
            tasks.drop(tasks.columns[cols],axis=1,inplace=True)
            if c == ord('r'):
                tasks = tasks[tasks['Status'] == "RUNNING"] 
            stdscr.addstr(3*gap+5, 0, tasks.to_string())
        except (IndexError, KeyError, OSError, AttributeError, ZeroDivisionError, requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            pass
        
        # Wait 1/2 of a second. Read below to learn about how to avoid problems with using time.sleep with getch!
        time.sleep(0.01)
        
wrapper(main)
