library(tidyverse)

fields = c("Timestamp", "Tag", "appId", "Epsilon", "Mu", "dbscanTime", "totalTime", "N")
data_path = "~/Documents/PhD/Research/Meetings/M02/ICPETesterStandalone.txt"
icpe = enframe(read_lines(data_path), name = "n", value = "line") %>% select(line) %>% 
  filter(grepl("local-", line)) %>%
  separate(line, into = fields, sep = "\\|") %>% 
  mutate(Epsilon = as.numeric(Epsilon), dbscanTime = as.numeric(dbscanTime), totalTime = as.numeric(totalTime)) %>%
  group_by(Epsilon) %>% summarise(dbscanTime = mean(dbscanTime), totalTime = mean(totalTime))

p = ggplot(data = icpe, aes(x = factor(Epsilon), y = totalTime, group = 1)) +
  geom_line() +
  geom_point() + 
  ylim(0, 8) +
  labs(title="Brinkhoff dataset - GRIndex stage", x="Epsilon [m]", y="Time [s]") 
plot(p)
 
ggsave("ICPETesterStandaloneGRIndex.pdf", width = 10, height = 6, device = "pdf")

p = ggplot(data = icpe, aes(x = factor(Epsilon), y = dbscanTime, group = 1)) +
  geom_line() +
  geom_point() + 
  ylim(0, 0.008) +
  labs(title="Brinkhoff dataset - DBScan stage", x="Epsilon [m]", y="Time [s]") 
plot(p)

ggsave("ICPETesterStandaloneDBScan.pdf", width = 10, height = 6, device = "pdf")