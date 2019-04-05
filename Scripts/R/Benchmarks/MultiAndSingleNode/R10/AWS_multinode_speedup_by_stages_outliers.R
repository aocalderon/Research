#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(lubridate)
require(tidyverse)
source("LogPreProcessor.R")

op <- options(digits.secs=3)
READ_DATA     = T
SAVE_PDF      = T
CHUNK_SIZE    = 32
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH  = "Scripts/R/Benchmarks/MultiAndSingleNode/R10/"
RESULTS_NAME  = "AWS_multinode_speedup_by_stages_outliers"
dataFile      = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.txt')

if(READ_DATA){
  data0 = readLines(dataFile)
  data0 = as_tibble(as.data.frame(data0, stringAsFactors = F)) %>% 
    rename(Line = data0) %>%
    filter(grepl("spark-submit| -> \\d\\.", Line)) 
  nchunks = (nrow(data0) / CHUNK_SIZE)
  chunks <- vector(mode="list", length=nchunks)
  for(n in 1:nchunks){
    start = (n - 1) * CHUNK_SIZE + 1
    end   = n * CHUNK_SIZE
    chunk = data0 %>% slice(start:end)
    params = parseParams(chunk %>% slice(1))
    epsilon = as.numeric(params["epsilon"])
    nodes = as.character(params["executors"])
    chunk = chunk %>%
      slice(2:CHUNK_SIZE) %>%
      separate(Line, c("Timestamp", "Info"), sep = " -> ") %>%
      separate(Info, c("Stage", "Time3", "Load"), sep = "\\|") %>%
      mutate(Timestamp = as.POSIXct(as.POSIXct(strptime(str_replace(Timestamp, ",", "."), "%Y-%m-%d %H:%M:%OS")))) %>%
      replace(is.na(.), "0") %>%
      mutate(Stage = str_trim(Stage), Load = as.numeric(Load), Time2 = as.numeric(Time3)) %>%
      mutate(t = c("0", sort(rep(1:5,6)))) %>%
      separate(Stage, c("stage_id", "stage_name"), "\\.") %>%
      unite("Stage", stage_id, t, stage_name, sep = ".") %>%
      mutate(Nodes = as.factor(rep(nodes, CHUNK_SIZE - 1))) %>%
      mutate(Epsilon = rep(epsilon, CHUNK_SIZE - 1)) %>%
      arrange(Timestamp) %>%
      select(Stage, Nodes, Epsilon, Time2)
    chunks[[n]] = chunk
  }
  data = bind_rows(chunks)
}

stats = data %>% group_by(Nodes, Stage) %>% summarise(lower = quantile(Time2)[2], upper = quantile(Time2)[4])
data2 = data %>% inner_join(stats, by = c("Nodes", "Stage")) %>% filter(Time2 > lower & Time2 < upper)
data3 = data2 %>% group_by(Nodes, Stage) %>% summarise(Time = mean(Time2), SD = sd(Time2))

title = "Execution time by Stage without outliers [Berlin_40K - 80K - 120K - 160K, Epsilon=110, Mu=3, Delta=3]"
g = ggplot(data=data3, aes(x=Stage, y=Time, fill=Nodes)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  geom_errorbar(aes(ymin=Time-SD, ymax=Time+SD), width=.3, position=position_dodge(width = 0.75)) +
  theme(axis.text.x = element_text(hjust=0, vjust=0.2, angle=270)) +
  labs(title=title, y="Time(s)", x="Stage")

if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.pdf'), width = 14, height = 8.50, dpi = 150, units = "in", device='pdf', g)
} else {
  plot(g)
}
options(op)