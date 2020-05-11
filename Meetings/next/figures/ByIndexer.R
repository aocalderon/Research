require(tidyverse)

log = enframe(readLines("ByIndexingLocal2.txt"))
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
  #mutate(appId = as.numeric(appId)) %>%
  select(appId, Epsilon, Partitions, Method, NPartitions) 

fieldsGeoTester = c("Timestamp","Tag1","appId", "Tag3","Method","Tag2","Time")
mf = log %>% filter(grepl(value, pattern = "\\|DJOIN\\|")) %>% 
  filter(grepl(value, pattern = "\\|Time\\|")) %>% 
  separate(value, fieldsGeoTester, sep = "\\|") %>%
  mutate(Time = as.numeric(Time)) %>%
  #mutate(appId = as.numeric(appId)) %>%
  select(appId, Method, Time)

data = mf %>% inner_join(spark, by = c("appId")) %>% select(Epsilon, Method.x, Time)

data2 = data %>% group_by(Method.x, Epsilon) %>% summarise(Time = mean(Time))
p = ggplot(data = data2, aes(x = Method.x, y = Time, fill = Epsilon)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Method", y="Time [s]", title="Execution time by Epsilon value") 
plot(p)
#ggsave("ByEpsilon.pdf", width = 10, height = 7, device = "pdf")
