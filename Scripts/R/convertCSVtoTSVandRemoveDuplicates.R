#!/usr/bin/Rscript
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, sqldf)

###################
# Setting global variables...
###################

RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
PATH = "Datasets/Berlin/"
DATASET = "B20K_d2"
EXTENSION = ".tsv"
SEPARATOR = "\t"
TRUNCATE_TO_INT = FALSE
ADD_T = FALSE
filename = paste0(RESEARCH_HOME,PATH,DATASET,EXTENSION)
data = read.table(filename, header = F, sep = SEPARATOR)

###################
# Reading data...
###################

data = as.data.table(data)
names(data) = c('id', 'x', 'y', 't')

###################
# Truncate decimal position if required...
###################

if(TRUNCATE_TO_INT){
  data$x = as.integer(data$x)
  data$y = as.integer(data$y)
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
write.table(data[ , c('id', 'x', 'y', 't')]
            , file = paste0(RESEARCH_HOME,PATH,DATASET,".tsv")
            , row.names = F
            , col.names = F
            , sep = '\t'
            , quote = F)
