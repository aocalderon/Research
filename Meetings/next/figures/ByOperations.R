require(tidyverse)

log = enframe(readLines("ByIndexer.txt"))
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

opFields = c("Timestamp", "Debug","MethodDebug","GlobalId","LocalId","Left","Right","Ops","appId")  
operations = log %>% filter(grepl(value, pattern = "DEBUG")) %>% 
  separate(value, opFields, sep = "\\|") %>%
  mutate(Left = as.numeric(Left), Right = as.numeric(Right), Ops = as.numeric(Ops)) %>%
  select(appId, MethodDebug, GlobalId, LocalId, Left, Right, Ops)

data0 = spark %>% inner_join(operations, by = c("appId")) 

data1 = data0 %>% filter(Partitions == 8 & Epsilon == 20)

# p = ggplot(data = data2, aes(x = Epsilon, y = avgOnPoints, fill = Method)) +
#   geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
#   theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
#   labs(x="Epsion", y="Opeartions [avg]", title="Average number of operations") 
# plot(p)
