import curses
from curses import wrapper
import time
from asciichartpy import plot
import random
import requests
import json
from lxml.html import parse
from console_progressbar import ProgressBar

def main(stdscr):
    # Make stdscr.getch non-blocking
    stdscr.nodelay(True)
    stdscr.clear()
    appID = open("/tmp/SparkAppID").readline()
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
        c = stdscr.getch()
        # Clear out anything else the user has typed in
        curses.flushinp()
        stdscr.clear()
        # Plot some data...
        response = requests.get("http://localhost:4040/api/v1/applications/{}/executors".format(appID))
        spark = json.loads(response.text)

        id = 0
        if spark[id]['id'] == 'driver':
            id = 1
        cores = spark[id]['totalCores']
        tasks = spark[id]['activeTasks']
        if tasks > cores:
            tasks = cores
        series0.append(tasks)
        if len(set(series0)) == 1:
            series0.append(set(series0).pop() + 1)
        stdscr.addstr(0, 0, "Executor {} at {}".format(id, spark[id]['hostPort']))
        stdscr.addstr(1, 0, plot(series0))
        stdscr.addstr(1 + max(series0) - tasks, width + 11, str(tasks))
        series0.reverse()
        series0 = series0[0:width]
        series0.reverse()

        if len(spark) > 2:
            id = 2
            tasks = spark[id]['activeTasks']
            if tasks > cores:
                tasks = cores
            series1.append(tasks)
            if len(set(series1)) == 1:
                series1.append(set(series1).pop() + 1)
            stdscr.addstr(gap, 0, "Executor {} at {}".format(id, spark[id]['hostPort']))
            stdscr.addstr(gap+1, 0, plot(series1))
            stdscr.addstr(gap+1 + max(series1) - tasks, width + 11, str(tasks))
            series1.reverse()
            series1 = series1[0:width]
            series1.reverse()

        if len(spark) > 3:
            id = 3
            tasks = spark[id]['activeTasks']
            if tasks > cores:
                tasks = cores
            series2.append(tasks)
            if len(set(series2)) == 1:
                series2.append(set(series2).pop() + 1)
            stdscr.addstr(2*gap, 0, "Executor {} at {}".format(id, spark[id]['hostPort']))
            stdscr.addstr(2*gap+1, 0, plot(series2))
            stdscr.addstr(2*gap+1 + max(series2) - tasks, width + 11, str(tasks))
            series2.reverse()
            series2 = series2[0:width]
            series2.reverse()
    
        # Stages...
        response = requests.get("http://localhost:4040/api/v1/applications/{}/stages".format(appID))
        stages   = json.loads(response.text)
        complete = 0
        active   = 0
        pending  = 0
        for stage in stages:
            status = stage['status']
            if status == 'COMPLETE':
                complete = complete + 1
            if status == 'PENDING':
                pending  = pending + 1
            if status == 'ACTIVE':
                active   = active + 1
                stageId  = stage['stageId']
                completeTasks = stage['numCompleteTasks']
                activeTasks   = stage['numActiveTasks']
                response = requests.get("http://localhost:4040/api/v1/applications/{}/stages/{}".format(appID, stageId))
                tasks = json.loads(response.text)
                totalTasks = len(tasks[0]['tasks'])
                name = tasks[0]['name']

        stagesBar = ProgressBar(total=100, prefix='Stages', suffix="{}/{}".format(complete, len(stages)), length=40, fill='#', zfill=' ')
        stdscr.addstr(3*gap+1, 0, stagesBar.generate_pbar((complete*100.0/len(stages))))
        try:
            complete, total = parse('http://localhost:4040/stages').getroot().get_element_by_id("activeStage-table").cssselect("div span")[1].text.strip().split("/")
            completeTasks = float(complete)
            totalTasks    = float(total)
            tasksBar = ProgressBar(total=100, prefix='Tasks', suffix="{}/{}".format(complete, total), length=40, fill='#', zfill=' ')
            stdscr.addstr(3*gap+3, 0, "[{}] {}".format(stageId, name))
            stdscr.addstr(3*gap+4, 0, tasksBar.generate_pbar((completeTasks*100.0)/totalTasks))
        except (IndexError, KeyError):
            pass
        
        # Wait 1/2 of a second. Read below to learn about how to avoid problems with using time.sleep with getch!
        time.sleep(0.25)
        
wrapper(main)
