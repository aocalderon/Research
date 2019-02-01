#!/usr/bin/Rscript

require(ggplot2)
require(stringr)
require(tidyverse)
require(scales)

READ_DATA     = T
SAVE_PDF      = T
SEP           = ";"
RESEARCH_HOME = Sys.getenv(c("RESEARCH_HOME"))

dataFile = paste0(RESEARCH_HOME, 'Scripts/R/Benchmarks/inter.txt')

data0 = read.table(dataFile, header = F, sep = ",")

data = as.tibble(data0, stringsAsFactors = F) %>%
  rename(Date = V1, Bogus = V2, Stage = V3, Epsilon = V4, Timestamp = V5, Load = V6) %>%
  select(Stage, Epsilon, Timestamp, Load) %>%
  mutate(Epsilon = as.numeric(Epsilon), Timestamp = as.numeric(Timestamp), Load = as.numeric(Load)) %>%
  group_by(Epsilon, Stage) %>% summarise(Load = sum(Load)) %>%
  filter(grepl("Candidates", Stage)) 

title = "Number of candidates after join..."
g = ggplot(data=data, aes(x=factor(Epsilon), y=Load, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.5),width = 0.5) +
  labs(title=title, y="# of Candidates", x=expression(paste(epsilon,"(mts)"))) +
  theme(legend.position="none") + scale_y_continuous(labels = comma)
plot(g)

if(SAVE_PDF){
  ggsave("./inter1.pdf", width = 7, height = 4, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}

data = as.tibble(data0, stringsAsFactors = F) %>%
  rename(Date = V1, Bogus = V2, Stage = V3, Epsilon = V4, Timestamp = V5, Load = V6) %>%
  select(Stage, Epsilon, Timestamp, Load) %>%
  mutate(Epsilon = as.numeric(Epsilon), Timestamp = as.numeric(Timestamp), Load = as.numeric(Load)) %>%
  group_by(Epsilon, Stage) %>% summarise(Load = sum(Load)) %>%
  filter(grepl("Less", Stage))

title = "Number of candidates after filtering..."
g = ggplot(data=data, aes(x=factor(Epsilon), y=Load, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.5),width = 0.5) +
  labs(title=title, y="# of Candidates", x=expression(paste(epsilon,"(mts)"))) +
  theme(legend.position="none") + scale_y_continuous(labels = comma)
plot(g)

if(SAVE_PDF){
  ggsave("./inter2.pdf", width = 7, height = 4, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}

data = as.tibble(data0, stringsAsFactors = F) %>%
  rename(Date = V1, Bogus = V2, Stage = V3, Epsilon = V4, Timestamp = V5, Load = V6) %>%
  select(Stage, Epsilon, Timestamp, Load) %>%
  mutate(Epsilon = as.numeric(Epsilon), Timestamp = as.numeric(Timestamp), Load = as.numeric(Load)) %>%
  group_by(Epsilon, Stage) %>% summarise(Load = sum(Load)) %>%
  filter(grepl("Flocks", Stage))

title = "Number of flocks reported..."
g = ggplot(data=data, aes(x=factor(Epsilon), y=Load, fill=Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.5),width = 0.5) +
  labs(title=title, y="# of Flocks", x=expression(paste(epsilon,"(mts)"))) +
  theme(legend.position="none") + scale_y_continuous(labels = comma)
plot(g)

if(SAVE_PDF){
  ggsave("./inter3.pdf", width = 7, height = 4, dpi = 300, units = "in", device='pdf', g)
} else {
  plot(g)
}

