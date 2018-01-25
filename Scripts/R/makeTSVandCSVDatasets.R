#!/usr/bin/Rscript
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, sqldf)

###################
# Setting global variables...
###################

RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
PATH = "Datasets/"
DATASET = "busesTest"
EXTENSION = ".txt"
SEPARATOR = "\t"
filename = paste0(RESEARCH_HOME,PATH,DATASET,EXTENSION)
data = read.table(filename, header = F, sep = SEPARATOR)

###################
# Reading data...
###################

data = as.data.table(data)
names(data) = c('id', 'x', 'y','t')

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
