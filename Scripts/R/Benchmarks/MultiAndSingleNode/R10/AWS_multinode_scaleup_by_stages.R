#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(lubridate)
require(tidyverse)

op <- options(digits.secs=3)
READ_DATA     = T
SAVE_PDF      = T
RUNS          = 25
NODES         = 4
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))
RESULTS_PATH = "Scripts/R/Benchmarks/MultiAndSingleNode/R9/"
RESULTS_NAME = "AWS_multinode_scaleup_by_stages"
dataFile = paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '.txt')

if(READ_DATA){
  data = readLines(dataFile)
  data = as_tibble(as.data.frame(data), stringAsFactors = F) %>% 
    rename(Line = data) %>%
    filter(grepl(" -> \\d\\.", Line)) %>%
    separate(Line, c("Timestamp", "Info"), sep = " -> ") %>%
    separate(Info, c("Stage", "Time3", "Load"), sep = "\\|") %>%
    mutate(Timestamp = as.POSIXct(as.POSIXct(strptime(str_replace(Timestamp, ",", "."), "%Y-%m-%d %H:%M:%OS")))) %>%
    replace(is.na(.), "0") %>%
    mutate(Stage = str_trim(Stage), Load = as.numeric(Load), Time2 = as.numeric(Time3)) %>%
    mutate(t = rep(c("0", sort(rep(1:5,6))), RUNS * NODES)) %>%
    separate(Stage, c("stage_id", "stage_name"), "\\.") %>%
    unite("Stage", stage_id, t, stage_name, sep = ".") %>%
    mutate(Nodes = as.factor(rep(sort(rep(1:4,31)), RUNS))) %>%
    arrange(Timestamp) %>%
    select(Stage, Nodes, Time2) 
  
  data2 = data %>%
    group_by(Nodes, Stage) %>% summarise(Time = mean(Time2), SD = sd(Time2))
}

title = "Execution time by Stage [Berlin_40K - 80K - 120K - 160K, Epsilon=110, Mu=3, Delta=3]"
g = ggplot(data=data2, aes(x=Stage, y=Time, fill=Nodes)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75),width = 0.75) +
  geom_errorbar(aes(ymin=Time-SD, ymax=Time+SD), width=.3, position=position_dodge(width = 0.75)) +
  theme(axis.text.x = element_text(hjust=0, vjust=0.2, angle=270)) +
  labs(title=title, y="Time(s)", x="Stage")

if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_bar.pdf'), width = 15, height = 8.50, dpi = 150, units = "in", device='pdf', g)
} else {
  plot(g)
}

title = "Boxplot by Stage [Berlin_40K - 80K - 120K - 160K, Epsilon=110, Mu=3, Delta=3]"
f = ggplot(data=data, aes(x=Stage, y=Time2, fill=Nodes)) +
  stat_boxplot(geom ='errorbar', width = 0.5, position = position_dodge(1)) +
  geom_boxplot(outlier.size = 0.5, position = position_dodge(1)) +
  theme(axis.text.x = element_text(hjust=0, vjust=0.2, angle=270)) +
  labs(title=title, y="Time(s)", x="Stage")
if(SAVE_PDF){
  ggsave(paste0(RESEARCH_HOME, RESULTS_PATH, RESULTS_NAME, '_boxplot.pdf'), width = 15, height = 8.50, dpi = 150, units = "in", device='pdf', f)
} else {
  plot(f)
}

options(op)