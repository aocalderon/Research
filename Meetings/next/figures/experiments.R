library(tidyverse)

fields = c("Timestamp", "Duration", "Method", "Dataset", "Epsilon", "Mu", "appId", "Millis", "Diff", "Tag", "Stage")
 
data = enframe(readLines("experiments.txt")) %>%
  filter(grepl(value, pattern = "TIME")) %>%
  separate(value, into = fields, sep = "\\|") %>%
  select(appId, Dataset, Epsilon, Mu, Stage, Method, Duration, Diff) %>%
  mutate(Dataset = paste0("Cell", Dataset),
         Epsilon = as.numeric(Epsilon),
         Mu = as.numeric(Mu),
         Duration = as.numeric(Duration)) %>%
  group_by(appId, Dataset, Epsilon, Mu, Stage, Method) %>% 
  summarise(Duration = mean(Duration))

data2 = data %>% pivot_wider(names_from = Stage, values_from = Duration) %>%
  mutate(A_Graph       = Graph,
         A_Cliques     = Cliques - Graph,
         A_MBC         = MBC - Cliques,
         A_Enclosed    = EnclosedByMBC - MBC,
         A_NotEnclosed = NotEnclosedByMBC - EnclosedByMBC,
         A_Prune       = Prune - NotEnclosedByMBC) %>%
  pivot_longer(cols = starts_with("A_"), names_prefix = "A_", names_to = "Stage", values_to = "Duration") %>%
  ungroup() %>% select(Dataset, Epsilon, Mu, Stage, Method, Duration) %>%
  mutate(Duration = Duration / 1000.0)

data2$Stage = factor(data2$Stage, levels = c("Graph", "Cliques", "MBC", "Enclosed", "NotEnclosed", "Prune"))

E = 3
dataByEpsilon = data2 %>% filter(Epsilon == E)
p = ggplot(data = dataByEpsilon, aes(x = Mu, y = Duration, fill = Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  facet_wrap(~Method) +
  labs(x="Mu [units]", y="Time [sec]", title=paste0("Performance by mu (epsilon=", E,"m)"))
 plot(p)
 ggsave(paste0("M_E", E, ".pdf"), device = "pdf")

