library(tidyverse)

fields = c("Timestamp", "Tag", "appId", "Cores", "Executors", "Epsilon", "Mu", "Partitions1", "Partitions2", "Time", "N")
data_path = "~/Documents/PhD/Research/Meetings/M01/ICPE_B0-1K.txt"
pflock = enframe(read_lines(data_path), name = "n", value = "line") %>% select(line) %>% 
  filter(grepl("\\|MAXIMALS\\|", line)) %>%
  separate(line, into = fields, sep = "\\|") %>% 
  mutate(Epsilon = as.numeric(Epsilon), Time = as.numeric(Time)) %>%
  group_by(Epsilon) %>% summarise(Time = mean(Time))

icpe1 = enframe(read_lines(data_path), name = "n", value = "line") %>% select(line) %>% 
  filter(grepl("ICPE", line)) %>% mutate(n = rep(1:500, each = 16)) 
fields1 = c("Timestamp", "Tag", "Duration", "Stage", "Time", "Load", "Status")
icpe2 = icpe1 %>%
  filter(grepl("Grid|DBScan|maximals", line)) %>%
  filter(grepl("END", line))  %>%
  separate(line, into = fields1, sep = "\\|") %>% 
  mutate(Time = as.numeric(Time)) %>%
  group_by(n) %>% summarise(Time = sum(Time))
icpe3 = icpe1 %>%
  filter(grepl("\\|ICPE  \\|", line))  
fields = c("Timestamp", "Tag", "appId", "Epsilon", "Mu", "Points", "Pairs", "Centers", "Disks", "TimeMaximals", "N")
icpe = icpe2 %>% inner_join(icpe3, by = "n") %>%
  separate(line, into = fields, sep = "\\|") %>% 
  mutate(Epsilon = as.numeric(Epsilon), Time = as.numeric(Time)) %>%
  group_by(Epsilon) %>% summarise(Time = mean(Time))
  
icpe$Method = "ICPE"
pflock$Method = "PFlock"

data = rbind(pflock, icpe)

p = ggplot(data = data, aes(x = factor(Epsilon), y = Time, group = Method)) +
  geom_line(aes(linetype=Method, color=Method)) +
  geom_point(aes(color=Method)) +
  labs(title="Brinkhoff sample 1 dataset", x="Epsilon [m]", y="Time [s]") 
plot(p)

ggsave("ICPE_B0-1K.pdf", width = 10, height = 6, device = "pdf")