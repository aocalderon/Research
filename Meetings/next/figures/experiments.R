require(tidyverse)
require(lubridate)
library(cowplot)

paramsPattern = "class|maxentries|epsilon|mu"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

log = enframe(readLines("experiments.txt"))
spark = log %>% filter(grepl(value, pattern = "SparkSubmit ")) %>% 
  separate(value, into = c("time", "duration", "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"class_name",NA,"epsilon",NA,"mu",NA,"entries"), sep = " ") %>%
  separate(class_name, into=c(NA,NA,NA,NA,"method"), sep="\\.") %>%
  select(appId, method, entries, epsilon, mu)
spark$method =  recode(spark$method, "MF_prime" = "BFE", "MF_CMBC" = "CMBC")

START = "\\|Points"
END   = "\\|Maximals"
pattern = paste0(START,"|",END)
fields = c("time", "duration", "appId", "msg")
times = log %>% filter(grepl(value, pattern = pattern)) %>%
  separate(value, fields, sep = "\\|") %>%
  mutate(duration = as.numeric(duration)) %>%
  mutate(time = parse_date_time(str_replace(time,",","."), "%Y-%m-%d %H:%M:%OS")) %>%
  separate(msg, into=c("stage",NA), sep="\t") %>%
  mutate(stage = str_trim(stage)) %>%
  select(appId, stage, time) %>%
  pivot_wider(names_from = stage, values_from = time) %>%
  mutate(time = difftime(Maximals, Points, units = "sec"))

data = spark %>% inner_join(times, by = "appId") %>%
  select(method, entries, epsilon, mu, time) %>%
  group_by(method, entries, epsilon, mu) %>% summarise(time = mean(time))
data$entries = factor(data$entries, levels = c("100", "200", "300", "400", "500"))
data$epsilon = factor(data$epsilon, levels = c("1", "3", "6", "9", "12", "15"))
data$mu = factor(data$mu, levels = c("4", "8", "12", "16"))

p = ggplot(data = data, aes(x = epsilon, y = time, fill = method)) +
 geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
 theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
 labs(x="Epsilon [m]", y="Time [sec]", title="Performance by epsilon") 
plot(p)

g = ggplot(data = data, aes(x = mu, y = time, fill = method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Mu [# of points]", y="Time [sec]", title="Performance by mu") 
plot(g)

# ggsave(paste0("data.pdf"), device = "pdf")