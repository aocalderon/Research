#!/usr/bin/Rscript

RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/Python/Tests/Test004.txt')

lines    = readLines(dataFile)
records  = c()
method   = ""
epsilon  = 0
mu       = 0
delta    = 0
time     = 0
readData = T

if(readData){
  for(line in lines){
    if(grepl(" LOG_", line)){
      params = str_split_fixed(line, " -> ", 2)[2]
      params = str_split_fixed(params, ",", 5)
      
      method  = strsplit(params[1], "_")[[1]][2]
      epsilon = str_trim(params[2])
      mu      = str_trim(params[3])
      delta   = str_trim(params[4])
      time    = str_trim(params[5])
      row = paste0(method,",", epsilon,",", mu,",", delta,",",time)
      print(row)
      records = c(records, row)
    }
  }
  data = as.data.frame(str_split_fixed(records, ",", 5))
  names(data) = c("Method", "Epsilon", "Mu", "Delta", "Time")
  data$Epsilon = as.numeric(as.character(data$Epsilon))
  data$Mu      = as.numeric(as.character(data$Mu))
  data$Delta   = as.numeric(as.character(data$Delta))
  data$Time    = as.numeric(as.character(data$Time))
}
