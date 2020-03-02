require(tidyverse)

log = enframe(readLines("ByIndexer.txt"))
paramsPattern = "indextype "
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}
spark = log %>% filter(grepl(value, pattern = "SparkSubmit")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Index"), sep = " ") %>%
  select(appId, Index)

fieldsGeoTester = c("Timestamp","Tag1","appId","Phase","Tag2","Time")
mf = log %>% filter(grepl(value, pattern = "\\|Time\\|")) %>% 
  separate(value, fieldsGeoTester, sep = "\\|") %>%
  mutate(Time = as.numeric(Time)) %>%
  select(appId, Phase, Time)

data = mf %>% inner_join(spark, by = c("appId")) %>% select(Index, Phase, Time)

data1 = data %>% group_by(Phase, Index) %>% summarise(Time = mean(Time))

p = ggplot(data = data1, aes(x = Phase, y = Time, fill = Index)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Phase", y="Time [s]", title="Execution time initial phases in MF by index type") 
plot(p)
ggsave("ByIndexer.pdf", width = 8, height = 7, device = "pdf")
