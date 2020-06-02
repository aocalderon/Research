require(tidyverse)

log = enframe(readLines("ByIndexer.txt"))
paramsPattern = "capacity|method"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

spark = log %>% filter(grepl(value, pattern = "SparkSubmit ")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Method",NA,"Capacity"), sep = " ") %>%
  #mutate(appId = as.numeric(appId)) %>%
  select(appId, Method, Capacity) 

fieldsGeoTester = c("Timestamp","Tag1","appId","Tag2","Tag3","Time")
mf = log %>% filter(grepl(value, pattern = "\\|Join done\\|")) %>% 
  filter(grepl(value, pattern = "\\|Time\\|")) %>% 
  separate(value, fieldsGeoTester, sep = "\\|") %>%
  mutate(Time = as.numeric(Time)) %>%
  #mutate(appId = as.numeric(appId)) %>%
  select(appId, Time)

data = mf %>% inner_join(spark, by = c("appId")) %>% select(Method, Capacity, Time)

dataP = data %>% filter(Method == "Partition") %>%
  group_by(Method, Capacity) %>% summarise(Time = mean(Time))

p = ggplot(data = dataP, aes(x = Capacity, y = Time, fill = Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Capacity", y="Time [s]", title="Execution time by capacity value") 
plot(p)
ggsave("ByCapacity.pdf", width = 10, height = 7, device = "pdf")

dataB = data %>% filter(Method == "Baseline") %>% select(Method, Time) %>%
  group_by(Method) %>% summarise(Time = mean(Time))
dataI = data %>% filter(Method == "Index") %>% select(Method, Time) %>%
  group_by(Method) %>% summarise(Time = mean(Time))
dataP = data %>% filter(Method == "Partition") %>% filter(Capacity == 250) %>% select(Method, Time) %>%
  group_by(Method) %>% summarise(Time = mean(Time))
dataJoin = bind_rows(dataB, dataI, dataP)

p = ggplot(data = dataJoin, aes(x = Method, y = Time)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Method", y="Time [s]", title="Execution time by distance join method") 
plot(p)
ggsave("ByMethod1.pdf", width = 10, height = 7, device = "pdf")

dataJoin = bind_rows(dataI, dataP)
p = ggplot(data = dataJoin, aes(x = Method, y = Time)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Method", y="Time [s]", title="Execution time by distance join method") 
plot(p)
ggsave("ByMethod2.pdf", width = 10, height = 7, device = "pdf")
