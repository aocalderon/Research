library(tidyverse)
library(ggplot2)

data0 = enframe(readLines("variants.txt"))

paramsPattern = "input"
getParams <- function(command){
  params = str_trim(str_split(command, "--")[[1]])
  params = params[grepl(paramsPattern, params)]
  return(paste(params, collapse = " "))
}
parseInput <- function(input){
  
}
fields = c("timestamp", "duration", "variant", "epsilon", "mu", "appId", "timestamp2", "duration2", "tag", "value")
scala = data0 %>% filter(grepl("COMMAND", value)) %>%
  separate(value, into = fields, sep = "\\|")
scala$params = scala$value %>% map(getParams)
scala = scala %>% separate(params, into = c(NA,"input"), sep = " ") %>%
  select(appId, input) %>% 
  separate(input, into = c(NA, "cell0"), sep = "sample") %>% 
  separate(cell0, into = c("cell", NA), sep = "\\.")

variants = data0 %>% filter(grepl("TIME", value)) %>%
  separate(value, into = fields, sep = "\\|") %>%
  select(appId, variant, epsilon, mu, value, duration) %>%
  mutate(duration = as.numeric(duration),
         epsilon = as.numeric(epsilon),
         mu = as.numeric(mu)
         )

data = scala %>% inner_join(variants, by = c("appId")) %>%
  pivot_wider(names_from = value, values_from = duration) %>%
  mutate(
    A_Graph = Graph - Read,
    A_Cliques = Cliques - Graph ,
    A_MBC = MBC - Cliques,
    A_Stats = Stats - MBC,
    A_Enclosed = EnclosedByMBC - Stats,
    A_NotEnclosed = NotEnclosedByMBC - EnclosedByMBC,
    A_Prune = Prune - NotEnclosedByMBC
  ) %>% 
  pivot_longer(cols = starts_with("A_"), names_to = "stage", names_prefix = "A_", values_to = "time") %>%
  select(variant, epsilon, mu, cell, stage, time) %>%
  group_by(cell, variant, epsilon, mu, stage) %>% summarise(time = mean(time)) %>% ungroup()

M=4
C="96"
S="NotEnclosed"
data95 = data %>% filter(cell == C & mu == M & stage == S)

p = ggplot(data = data95, aes(x = factor(epsilon), y = time, fill = variant)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  #facet_grid(~mu) +
  labs(x="Epsilon", y="Time [ms]", title=paste0("Performance by epsilon (mu=", M,")"))
plot(p)
ggsave(paste0("TC_", C, "E_", E,"_M", M, ".pdf"), device = "pdf")
