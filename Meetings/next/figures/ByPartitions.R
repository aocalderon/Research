require(tidyverse)
require(ssh)

log = enframe(readLines("partitions.tsv"))
paramsPattern = "partition|factor"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}

spark = log %>% filter(grepl(value, pattern = "SparkSubmit ")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Partitions",NA,"Factor", NA, NA), sep = " ") %>%
  select(appId, Partitions, Factor)

fieldsGeoTester = c("Timestamp", "Tag", "appId", "Phase","Tag2","Duration")
mf = log %>% filter(grepl(value, pattern = "Disks phase\\|Time")) %>% 
  separate(value, fieldsGeoTester, sep = "\\|") %>%
  mutate(Duration = as.numeric(Duration)) %>%
  #mutate(appId = as.numeric(appId)) %>%
  select(appId, Duration)

data = mf %>% inner_join(spark, by = c("appId")) %>%
  group_by(Factor, Partitions) %>% summarise(Duration = mean(Duration)) %>%
  mutate(Partitions = factor(Partitions, levels = c("540","648","756","864","972","1080","1188","1296","1404","1512","1620"))) %>%
  mutate(Duration = as.numeric(Duration))

p = ggplot(data = data, aes(x = Partitions, y = Duration)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Partitions", y="Time [s]", title="Execution time by Number of Partitions") 
plot(p)

ggsave(paste0("ByPartitions.pdf"), width = 12, height = 8, device = "pdf")





