#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)

READ_DATA     = T
SAVE_PDF      = T
SEP           = ","
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/Python/Tests/Test008.txt')

lines    = readLines(dataFile)
records  = c()
method   = ""
epsilon  = 0
mu       = 0
delta    = 0
time     = 0

if(READ_DATA){
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
  
  data = as.tibble(str_split_fixed(records,SEP, 5), stringsAsFactors = F) %>%
    rename(Method = V1, Epsilon = V2, Mu = V3, Delta = V4, Time = V5) %>%
    mutate(Epsilon = as.numeric(Epsilon), Mu = as.numeric(Mu), Delta = as.numeric(Delta), Time = as.numeric(Time))
}

title = "Execution time by Epsilon"
g = ggplot(data=data, aes(x=factor(Epsilon), y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  labs(title=title, y="Time(s)", x=expression(paste(epsilon,"(mts)"))) 
if(SAVE_PDF){
  ggsave("./BFEvsPFLOCK_ML.pdf", width = 7, height = 5, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}
