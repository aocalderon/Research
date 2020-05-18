require(tidyverse)

log = enframe(readLines("Test.txt"))
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
  select(appId, Epsilon, Partitions, Method) 

fieldsGeoTester = c("Timestamp","Tag1","appId", "Tag3","Method","Tag2","Time")
mf = log %>% filter(grepl(value, pattern = "\\|DJOIN\\|")) %>% 
  filter(grepl(value, pattern = "\\|Time\\|")) %>% 
  separate(value, fieldsGeoTester, sep = "\\|") %>%
  mutate(Time = as.numeric(Time)) %>%
  #mutate(appId = as.numeric(appId)) %>%
  select(appId, Time)

data = mf %>% inner_join(spark, by = c("appId")) %>% select(Method, Epsilon, Partitions, Time)

data1 = data %>% group_by(Method, Epsilon, Partitions) %>% summarise(Time = mean(Time))
data1$Partitions = factor(data1$Partitions, levels = c("1", "4", "8", "16"))
data2 = data1 %>% filter(Partitions != "1")

p = ggplot(data = data2, aes(x = Partitions, y = Time, fill = Method)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  facet_wrap(~Epsilon) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Partitions", y="Time [s]", title="Execution time") 
plot(p)
ggsave("Test.pdf", width = 10, height = 7, device = "pdf")
