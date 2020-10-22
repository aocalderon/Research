library(tidyverse)

data = read_delim("log_M3_E5-30.txt", "|", col_names = c('Variant', 'Epsilon', 'Cliques', "Time")) %>%
  mutate(Time = as.numeric(Time)) %>%
  group_by(Variant, Epsilon) %>% summarise(Time = mean(Time))


p = ggplot(data = data, aes(x = Epsilon, y = Time, group = Variant)) +
  geom_line(aes(color = Variant)) +
  geom_point(aes(color = Variant)) +
  #geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Epsilon [m]", y="Time [s]", title="Execution time by Variant")
#plot(p)
#ggsave(paste0("Variant3.pdf"), width = 12, height = 8, device = "pdf")

data = read_delim("log_M2_E10-50.txt", "|", col_names = c('Variant', 'Epsilon', 'Cliques', "Time")) %>%
  mutate(Time = as.numeric(Time)) %>%
  group_by(Variant, Epsilon) %>% summarise(Time = mean(Time))

p = ggplot(data = data, aes(x = Epsilon, y = Time, fill = Variant)) +
  geom_line(aes(color = Variant)) +
  geom_point(aes(color = Variant)) +
  #geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Epsilon [m]", y="Time [s]", title="Execution time by Variant")
#plot(p)
#ggsave(paste0("Variant2.pdf"), width = 12, height = 8, device = "pdf")

data = read_delim("log_M2_E10-50.txt", "|", col_names = c('Variant', 'Epsilon', 'Cliques', "Time")) %>%
  mutate(Cliques = as.numeric(Cliques)) %>%
  group_by(Epsilon) %>% summarise(Cliques = max(Cliques))


p = ggplot(data = data, aes(x = Epsilon, y = Cliques)) +
  #geom_line(aes(color = Variant)) +
  #geom_point(aes(color = Variant)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x="Epsilon [m]", y="# of Cliques", title="Numbero of maximal cliques found")
plot(p)
ggsave(paste0("Cliques.pdf"), width = 12, height = 8, device = "pdf")