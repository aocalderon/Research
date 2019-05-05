library(tidyverse)
library(plotly)

getStarts <- function(d){
  s = d %>% select(Stage, Node, Duration, Tasks) %>% 
    group_by(Stage, Node) %>% 
    summarise(Start=min(Duration), Tasks=min(Tasks)) %>% arrange(Start)
  return(s)
}

RESEARCH_HOME = "/home/and/Documents/PhD/Research"
lines = readLines(paste0(RESEARCH_HOME, "/Scripts/Misc/tmp/monitor2.txt"))
lines = lines[grepl("|", lines)]
monitor = as_tibble(lines) %>%
  separate(value, into=c("Timestamp", "Duration", "ID", "Executors", "Cores", "Node", "Stage", "RDDs", "Tasks", "Times", "Load"), sep="\\|") %>%
  separate(ID, into=c(NA, NA, "ID"), sep="-") %>%
  filter(grepl(":", Node)) %>%
  separate(Node, into=c("Node", NA), sep=":") %>%
  select(ID, Duration, Executors, Node, Stage, RDDs, Tasks, Load) %>%
  mutate(Duration=as.numeric(Duration), Node=paste0(Executors, ":", Node), RDDs=as.numeric(RDDs), Tasks=as.numeric(Tasks), Load=as.numeric(Load)) %>%
  group_by(ID, Duration, Node, Executors, Stage) %>% summarise(RDDs=mean(RDDs), Tasks=mean(Tasks), Load=mean(Load))

d = monitor %>% filter(ID == "0048" || ID == "0049" || ID == "0050") %>% ungroup %>% 
  select(Duration, Tasks, Node, Executors, Stage) %>% 
  mutate(Node=factor(Node), Nodes=factor(Executors)) %>% 
  arrange(Duration, Tasks)

head(d)

p = ggplot(data = d, aes(x = Duration, y = Tasks, group = Node)) +
  geom_line(aes(color = Nodes, linetype = Nodes)) +
  geom_point(data = getStarts(d), aes(x=Start, y=Tasks, group=Node, color=Node
                                      , text = paste(Stage,"<br>Start:",Start,"<br>",Tasks)))
             
ggplotly(p, tooltip = c("text"))

n <- 10
d <- data.frame(x = 1:n, y = rnorm(n))
ggplot(d,aes(x,y)) + geom_point() + 
  geom_line(data=data.frame(spline(d, n=n*10)))
 
# lines = readLines(paste0(RESEARCH_HOME, "/Scripts/Misc/tmp/nohup2.txt"))
# lines = lines[grepl("\\|Session|\\|Data|[A-H]\\.", lines)]
# log = as_tibble(lines) %>%
#   separate(value, into=c("Timestamp", "ID", "Duration", "Stage", "Time", "Load", "Bogus"), sep="\\|") %>%
#   separate(ID, into=c(NA, NA, "ID"), sep="-") %>%
#   select(ID, Duration, Stage) %>%
#   mutate(Duration = as.numeric(str_trim(Duration)), Stage = str_trim(Stage))
# head(log)