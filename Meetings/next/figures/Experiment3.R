require(tidyverse)

log = enframe(readLines("Experiment3.txt"))
paramsPattern = "epsilon |parallelism |partitions "
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}
spark = log %>% filter(grepl(value, pattern = "SparkSubmit")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Epsilon", NA,"Partitions", NA,"Parallelism", NA,"Partitions2"), sep = " ") %>%
  select(appId, Epsilon, Partitions, Parallelism, Partitions2) 

fieldsGeoTester = c("Timestamp","Tag1","appId","Phase","Tag2","Time")
mf = log %>% filter(grepl(value, pattern = "\\|Time\\|")) %>% 
  separate(value, fieldsGeoTester, sep = "\\|") %>%
  mutate(Time = as.numeric(Time)) %>%
  select(appId, Phase, Time)

data = mf %>% inner_join(spark, by = c("appId"))

data1 = data %>% group_by(Phase, Partitions, Epsilon) %>% summarise(Time = mean(Time))

p = ggplot(data = data1, aes(x = factor(Partitions), y = Time, fill = Epsilon)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  facet_wrap(~Phase, ncol = 1) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Number of partitions", y="Time [s]", title="Execution time initial phases MF") 
plot(p)
ggsave("Experiment3.pdf", width = 8, height = 7, device = "pdf")
  