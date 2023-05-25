library(tidyverse)

data0 = enframe(readLines("~/Datasets/PFlocks/experiments6.txt"))

getParams <- function(command, pattern){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(pattern, params)]
  return(paste(params, collapse = " "))
}
parseValue <- function(x){
  y = str_split(x, "=")[[1]]
  return(as.numeric(y[2]))
}

fields = c("stamp", "millis", "variant", "epsilon", "mu", "appId", "stamp2", "duration", "tag", "value")
cliqueParams = data0 %>% filter(grepl("COMMAND", value)) %>%
  separate(value, into = fields, sep = "\\|") %>% 
  mutate(params = value %>% map(getParams, "input")) %>% 
  separate(params, into = c(NA, "input"), sep = " ") %>%
  select(appId, epsilon, mu, input) %>% 
  separate(input, into = c(NA, "cell0"), sep = "sample") %>% 
  separate(cell0, into = c("cell", NA), sep = "\\.")
 
fieldsStats = c("stamp1", "millis", "variant", "epsilon", "mu", "appId", "stamp2", "duration", "tag", "stats")
cliqueStats = data0 %>% filter(grepl("mode=2", value)) %>%
  separate(value, into = fieldsStats, sep = "\\|") %>%
  separate(stats, into = c(NA,"min","max","avg","sd","n",NA,NA), sep = ";") %>%
  select(appId, min, max, avg, sd, n) %>%
  mutate(min = map(min, parseValue),
         max = map(max, parseValue),
         avg = map(avg, parseValue),
         sd =  map(sd, parseValue),
         n =   map(n, parseValue)) %>% 
  unnest(min)%>% unnest(max)%>% unnest(avg)%>% unnest(sd) %>% unnest(n)


## EACH
fieldsEachs = c("stamp1", "millis", "variant", "epsilon", "mu", "appId", "stamp2", "duration", "tag", "stage")
eachs = data0 %>% filter(grepl("EACH", value)) %>%
  separate(value, into = fieldsEachs, sep = "\\|") %>%
  mutate(duration = as.numeric(duration))

eachsTime1 = eachs %>% filter(tag == "TIME2") %>% 
  select(appId, stage, duration) %>%
  group_by(appId, stage) %>% 
  summarise(avg_time = mean(duration),
            tot_time = sum(duration))

eachsTime2 = eachsTime1 %>% group_by(appId) %>% summarise(avg_time = sum(avg_time), tot_time = sum(tot_time))

## COLLECTS
collects = data0 %>% filter(grepl("COLLECT", value)) %>%
  separate(value, into = fieldsEachs, sep = "\\|") %>%
  mutate(duration = as.numeric(duration))

collectsTime1 = collects %>% filter(tag == "TIME2") %>% 
  select(appId, stage, duration) %>%
  group_by(appId, stage) %>% 
  summarise(avg_time = mean(duration),
            tot_time = sum(duration))

collectsTime2 = collectsTime1 %>% group_by(appId) %>% summarise(avg_time = sum(avg_time), tot_time = sum(tot_time))

## HASH
hashs = data0 %>% filter(grepl("HASH_3", value)) %>%
  separate(value, into = fieldsEachs, sep = "\\|") %>%
  mutate(duration = as.numeric(duration))

hashsTime1 = hashs %>% filter(tag == "TIME2") %>% 
  select(appId, stage, duration) %>%
  group_by(appId, stage) %>% 
  summarise(avg_time = mean(duration),
            tot_time = sum(duration))

hashsTime2 = hashsTime1 %>% group_by(appId) %>% summarise(avg_time = sum(avg_time), tot_time = sum(tot_time))

#variants = data0 %>% filter(grepl("TIME2", value)) %>%
#  separate(value, into = fields, sep = "\\|") %>%
#  select(appId, variant, epsilon, mu, value, duration) %>%
#  mutate(time = as.numeric(duration),
#        epsilon = as.numeric(epsilon),
#        mu = as.numeric(mu),
#        stage = value)
 
#data = variants %>% inner_join(scala) %>%
#  select(variant, epsilon, mu, cell, stage, time) %>%
#  group_by(cell, variant, epsilon, mu, stage) %>% summarise(time = mean(time)) %>% ungroup()

# p = ggplot(data = data95, aes(x = factor(epsilon), y = time, fill = variant)) +
#   geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
#   theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
#   #facet_grid(~mu) +
#   labs(x="Epsilon", y="Time [ms]", title=paste0("Performance by epsilon (mu=", M,")"))
# plot(p)
# ggsave(paste0("C_", C, "E_", E,"_M", M, ".pdf"), device = "pdf")
