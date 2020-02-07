require(tidyverse)

data = read_tsv("~/Documents/PhD/Research/Meetings/M_prime/MF-data.txt", col_names = F) %>%
  rename(appId = X1, Phase = X2, Time = X3, Instant = X4, Finer = X5, Coarse = X6) %>% 
  group_by(Phase, Coarse, Finer) %>% summarise(Time = mean(Time)) 

p = ggplot(data = data, aes(x = factor(Finer), y = Time, fill = factor(Phase))) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  facet_wrap(~Coarse, ncol = 1) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Number of partitions in finer grid", y="Time [s]", title="Execution time for maximal disk finding",
       subtitle="Per number of partition in coarse grid", fill="Phases") 
plot(p)

ggsave("~/Documents/PhD/Research/Meetings/M_prime/MF-data.pdf", width = 8, height = 7, device = "pdf")

# data2 = read_tsv("~/Documents/PhD/Research/Meetings/M_prime/MF-data.txt", col_names = F) %>%
#   rename(appId = X1, Phase = X2, Time = X3, Instant = X4, Finer = X5, Coarse = X6) %>% 
#   group_by(appId, Coarse, Finer) %>% summarise(Time = sum(Time))
# g = ggplot(data = data2, aes(x = factor(Finer), y = Time)) +
#   geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
#   facet_wrap(~Coarse, ncol = 1) +
#   theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
#   labs(x="Number of partitions in finer grid", y="Time [s]", title="Execution time for maximal disk finding") 
# plot(g)