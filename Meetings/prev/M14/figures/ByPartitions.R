require(tidyverse)

## Reading log
log = enframe(readLines("ByIndexer6.txt"))
spark = log %>% filter(grepl(value, pattern = "SparkSubmit")) %>% 
  separate(value, into = c(NA, "appId", "command"), sep = "\\|")

## Reading parameters values...
paramsPattern = "capacity|fraction"
defaultCapacity = 100
defaultFraction = 0.025
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}
spark$params = spark$command %>% map(getParams)
spark = spark %>% separate(params, into = c(NA,"Capacity",NA,"Fraction"), sep = " ") %>%
  mutate(appId = as.numeric(appId)) %>%
  select(appId, Capacity, Fraction) %>%
  mutate(Capacity = as.numeric(Capacity), Fraction = as.numeric(Fraction))

## Reading stats
statsFields = c("appId", "geom", "partitionId", "sizeA", "sizeB", "quadtree", "locateA", "locateB", "pairs")
stats = enframe(readLines("Stats.txt")) %>% 
  separate(value, into = statsFields, sep = "\t") %>%
  mutate(appId = as.numeric(appId), sizeA = as.numeric(sizeA), sizeB = as.numeric(sizeB)) %>%
  mutate(quadtree = as.numeric(quadtree), locateA = as.numeric(locateA), locateB = as.numeric(locateB), pairs = as.numeric(pairs))

## Join data
data = stats %>% inner_join(spark, by = c("appId")) %>% 
  select(appId, partitionId, sizeA, sizeB, quadtree, locateA, locateB, pairs, Capacity, Fraction) %>%
  filter(Capacity == defaultCapacity & Fraction == defaultFraction)

dataByPartitionSize = data %>%
  filter(appId == "3699") %>%
  