#!/usr/bin/Rscript
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, sqldf)

###################
# Setting global variables...
###################

RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
PATH = "Datasets/"
DATASET = "Berlin_N277K_A18K_T15"
EXTENSION = ".csv"
SEPARATOR = ","
TRUNCATE_TO_INT = FALSE
ROUND_TO_DECIMALS = 2
ADD_T = FALSE
filename = paste0(RESEARCH_HOME,PATH,DATASET,EXTENSION)
data = read.table(filename, header = F, sep = SEPARATOR)

###################
# Reading data...
###################

data = as.data.table(data)
names(data) = c('x', 'y','t','id')
##
data$t = data$t -117

###################
# Truncate decimal position if required...
###################

if(TRUNCATE_TO_INT){
  data$x = as.integer(data$x)
  data$y = as.integer(data$y)
}

###################
# Round to x decimals if required...
###################

if(ROUND_TO_DECIMALS != -1){
  data$x = round(data$x, ROUND_TO_DECIMALS)
  data$y = round(data$y, ROUND_TO_DECIMALS)
}

###################
# Adding temporal dimension if required...
###################

if(ADD_T){
  data$t = 0
}

###################
# Prunning possible duplicates...
###################

data = data[ , list(id = min(id)), by = c('x', 'y', 't')]

###################
# Writing back...
###################
TIMESTAMPS = 10
for(i in seq(0,TIMESTAMPS)){
  write.table(data[data$t == i , c('id', 'x', 'y', 't')]
              , file = paste0(RESEARCH_HOME,PATH,DATASET,i,"-",i,".tsv")
              , row.names = F
              , col.names = F
              , sep = '\t'
              , quote = F)
}
for(i in seq(0,TIMESTAMPS)){
  write.table(data[data$t <= i , c('id', 'x', 'y', 't')]
              , file = paste0(RESEARCH_HOME,PATH,DATASET,"0-",i,".tsv")
              , row.names = F
              , col.names = F
              , sep = '\t'
              , quote = F)
}
