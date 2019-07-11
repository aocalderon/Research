#!/usr/bin/Rscript
library(tidyverse)

###################
# Reading data...
###################

filename = "/home/and/Documents/PhD/Research/Datasets/LA/LA_sample.tsv"
data = read_tsv(filename, col_names = c('id', 'x', 'y', 't'))
show(data)
print(paste0("Data count: ", nrow(data)))

###################
# Prunning possible duplicates...
###################

data1 = data %>% group_by(x, y, t) %>% summarise(id = min(id)) %>% select(id, x, y ,t) %>% arrange(t)
show(data1)  
print(paste0("Data count: ", nrow(data1)))

###################
# Writing back...
###################
data1 %>% write_tsv("/home/and/Documents/PhD/Research/Datasets/LA/LA_sample_clean.tsv", col_names = F)
