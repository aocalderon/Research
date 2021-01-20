require(tidyverse)

log = enframe(readLines("ByIndexer6.txt"))
paramsPattern = "capacity|fraction"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

spark = log %>% filter(grepl(value, pattern = "SparkSubmit")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Capacity",NA,"Fraction"), sep = " ") %>%
  mutate(appId = as.numeric(appId)) %>%
  select(appId, Capacity, Fraction) 

fieldsGeoTester = c("Timestamp","Tag1","appId", "Tag3","Phase","Tag2","Time")
mf = log %>% filter(grepl(value, pattern = "\\|DJOIN\\|")) %>% 
  filter(grepl(value, pattern = "\\|Time\\|")) %>% 
  separate(value, fieldsGeoTester, sep = "\\|") %>%
  mutate(Time = as.numeric(Time), appId = as.numeric(appId)) %>%
  select(appId, Phase, Time)

data = mf %>% inner_join(spark, by = c("appId")) %>% select(Capacity, Fraction, Phase, Time)

defaultFraction = "0.025"
data1 = data %>% 
  filter(Fraction == defaultFraction) %>% 
  group_by(Phase, Capacity, Fraction) %>% summarise(Time = mean(Time))
data1$Capacity = factor(data1$Capacity, levels = sort(as.numeric(unique(data1$Capacity))))
p = ggplot(data = data1, aes(x = Phase, y = Time, fill = Capacity)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Method", y="Time [s]", title="Execution time by maximum items per grid (capacity)") 
plot(p)
ggsave("ByCapacity.pdf", width = 10, height = 7, device = "pdf")

defaultCapacity = "100"
data2 = data %>% 
  filter(Capacity == "100") %>% 
  group_by(Phase, Fraction) %>% summarise(Time = mean(Time))
p = ggplot(data = data2, aes(x = Phase, y = Time, fill = Fraction)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Method", y="Time [s]", title="Execution time by size of the sample (fraction)") 
plot(p)
ggsave("ByFraction.pdf", width = 10, height = 7, device = "pdf")
