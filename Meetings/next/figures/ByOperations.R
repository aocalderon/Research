require(tidyverse)

log = enframe(readLines("ByIndexingLocal.txt"))
paramsPattern = "epsilon|partition|method"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

spark = log %>% filter(grepl(value, pattern = "SparkSubmit ")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Partitions",NA,"Epsilon",NA,"Method",NA,"NPartitions"), sep = " ") %>%
  select(appId, Epsilon, Partitions, Method, NPartitions) %>%
  mutate(Epsilon = as.numeric(Epsilon), Partitions = as.numeric(Partitions))

opFields = c("Debug","Method","GlobalId","LocalId","Left","Right","Ops","appId")  
operations = log %>% filter(grepl(value, pattern = "DEBUG")) %>% 
  separate(value, opFields, sep = "\\|") %>%
  mutate(Left = as.numeric(Left), Right = as.numeric(Right), Ops = as.numeric(Ops)) %>%
  group_by(Method, GlobalId, appId) %>% # Total number of operations per partition...
  summarise(Left = sum(Left), Right = sum(Right), Ops = sum(Ops)) %>% 
  group_by(Method, appId) %>% # Minimum, maximum and average number of operation per core...
  summarise(minLeft  = min(Left),  maxLeft  = max(Left),  avgLeft  = mean(Left), 
            minRight = min(Right), maxRight = max(Right), avgRight = mean(Right),
            minOps   = min(Ops),   maxOps   = max(Ops),   avgOps   = mean(Ops)
            )
data0 = spark %>% inner_join(operations, by = c("appId")) %>% mutate(Method = Method.x)

data1 = data0 %>% group_by(Epsilon, Partitions, Method) %>%
  summarise(maxOnCenters  = mean(maxLeft),  avgOnCenters  = mean(avgLeft),
            maxOnPoints   = mean(maxRight), avgOnPoints   = mean(avgRight),
            maxOperations = mean(maxOps),   avgOperations = mean(avgOps)
            )
data2 = data1 %>% filter(Partitions == 16)
p = ggplot(data = data2, aes(x = Epsilon, y = avgOnPoints, fill = Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Epsion", y="Opeartions [avg]", title="Average number of operations") 
plot(p)
