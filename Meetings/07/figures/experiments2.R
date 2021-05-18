library(tidyverse)

fields = c("None", "Log", "Dataset", "Epsilon", "Mu", "appId", "Timestamp", "Duration", "Tag", "Stage")
 
data = enframe(readLines("experiments.txt")) %>%
  filter(grepl(value, pattern = "TIME")) %>%
  separate(value, into = fields, sep = "\\|") %>%
  select(appId, Dataset, Epsilon, Mu, Duration, Stage) %>%
  mutate(Dataset = paste0("Cell", Dataset),
         Epsilon = as.numeric(Epsilon),
         Mu = as.numeric(Mu),
         Duration = as.numeric(Duration)) %>%
  group_by(Dataset, Epsilon, Mu, Stage) %>% summarise(Duration = mean(Duration))

data2 = data %>% pivot_wider(names_from = Stage, values_from = Duration) %>%
  mutate(A_Graph       = Graph,
         A_Cliques     = Cliques - Graph,
         A_MBC         = MBC - Cliques,
         A_Enclosed    = EnclosedByMBC - MBC,
         A_NotEnclosed = NotEnclosedByMBC - EnclosedByMBC,
         A_Prune       = Prune - NotEnclosedByMBC) %>%
  pivot_longer(cols = starts_with("A_"), names_prefix = "A_", names_to = "Stage", values_to = "Duration") %>%
  select(Dataset, Epsilon, Mu, Stage, Duration) %>%
  mutate(Duration = Duration / 1000.0)

data2$Stage = factor(data2$Stage, levels = c("Graph", "Cliques", "MBC", "Enclosed", "NotEnclosed", "Prune"))

M = 4
dataByMu = data2 %>% filter(Mu == M)
p = ggplot(data = dataByMu, aes(x = factor(Epsilon), y = Duration, fill = Stage)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Epsilon [m]", y="Time [sec]", title=paste0("Performance by epsilon (mu=", M,")"))
 plot(p)
 ggsave(paste0("E_M", M, ".pdf"), device = "pdf")

