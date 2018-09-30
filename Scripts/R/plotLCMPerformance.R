#!/usr/bin/Rscript

if (!require("pacman")) install.packages("pacman")
pacman::p_load(ggplot2, data.table, foreach, sqldf, tidyr, stringr, dplyr, anytime)
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/Python/nohup.out')

lines = readLines(dataFile)
records = c()
method = ""
epsilon = 0
mu = 0
delta = 0
stage = ""
time = 0
readData = TRUE
uno_time_end_flag = FALSE

if(readData){
  for(line in lines){
    if(grepl("LOG Running LCMuno...", line, perl = T)){
      uno_time_start = str_split_fixed(line, "->", 2)[1]
      uno_time_end_flag = TRUE
    } else if(uno_time_end_flag){
      uno_time_end = str_split_fixed(line, "->", 2)[1]
      uno_time_end_flag = FALSE
    }
  }
}
