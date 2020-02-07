require(tidyverse)

data = read_tsv("~/Documents/PhD/Research/Meetings/M_prime/FF-data_2020-01-29.txt", col_names = F) %>%
  rename(appId = X1, Phase = X2, Time = X3, Instant = X4, Coarse = X5, Finer = X6) %>% 
  group_by(appId, Instant, Phase, Coarse, Finer) %>% summarise(Time = sum(Time)) %>%
  group_by(appId, Phase, Coarse, Finer) %>% summarise(Time = mean(Time)) %>% filter(Coarse >= 100)

p = ggplot(data = data, aes(x = factor(Finer), y = Time, fill = factor(Phase))) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  facet_wrap(~Coarse, ncol = 1) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Number of partitions in finer grid", y="Time [s]", title="Execution time for flock finding",
       subtitle="Per number of partition in coarse grid", fill="Phases") 
plot(p)

ggsave("~/Documents/PhD/Research/Meetings/M_prime/FF-data.pdf", width = 9, height = 8, device = "pdf")