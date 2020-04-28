require(tidyverse)

log = enframe(readLines("ByIndexer7.txt"))
paramsPattern = "epsilon"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

spark = log %>% filter(grepl(value, pattern = "SparkSubmit")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Epsilon"), sep = " ") %>%
  mutate(appId = as.numeric(appId)) %>%
  select(appId, Epsilon) 

fieldsGeoTester = c("Timestamp","Tag1","appId", "Tag3","Phase","Tag2","Time")
mf = log %>% filter(grepl(value, pattern = "\\|DJOIN\\|")) %>% 
  filter(grepl(value, pattern = "\\|Time\\|")) %>% 
  separate(value, fieldsGeoTester, sep = "\\|") %>%
  mutate(Time = as.numeric(Time), appId = as.numeric(appId)) %>%
  select(appId, Phase, Time)

data = mf %>% inner_join(spark, by = c("appId")) %>% select(Epsilon, Phase, Time)

data2 = data %>% group_by(Phase, Epsilon) %>% summarise(Time = mean(Time))
p = ggplot(data = data2, aes(x = Phase, y = Time, fill = Epsilon)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Method", y="Time [s]", title="Execution time by Epsilon value") 
plot(p)
ggsave("ByEpsilon.pdf", width = 10, height = 7, device = "pdf")
