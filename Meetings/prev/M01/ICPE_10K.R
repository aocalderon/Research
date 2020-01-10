library(tidyverse)

fields = c("Timestamp", "Tag", "appId", "Cores", "Executors", "Epsilon", "Mu", "Partitions1", "Partitions2", "Time", "N")
data_path = "~/Documents/PhD/Research/Meetings/M01/ICPE_10K.txt"
pflock = enframe(read_lines(data_path), name = "n", value = "line") %>% select(line) %>% 
  filter(grepl("\\|MAXIMALS\\|", line)) %>%
  separate(line, into = fields, sep = "\\|") %>% 
  mutate(Epsilon = as.numeric(Epsilon), Time = as.numeric(Time)) %>%
  group_by(Epsilon) %>% summarise(Time = mean(Time))

fields = c("Timestamp", "Tag", "appId", "Epsilon", "Mu", "Points", "Pairs", "Centers", "Disks", "Time", "N")
icpe = enframe(read_lines(data_path), name = "n", value = "line") %>% select(line) %>% 
  filter(grepl("\\|ICPE  \\|", line)) %>%
  separate(line, into = fields, sep = "\\|") %>% 
  mutate(Epsilon = as.numeric(Epsilon), Time = as.numeric(Time)) %>%
  group_by(Epsilon) %>% summarise(Time = mean(Time))

icpe$Method = "ICPE"
pflock$Method = "PFlock"

data = rbind(pflock, icpe)

p = ggplot(data = data, aes(x = factor(Epsilon), y = Time, group = Method)) +
  geom_line(aes(linetype=Method, color=Method)) +
  geom_point(aes(color=Method)) +
  labs(title="LA_10K dataset", x="Epsilon [m]", y="Time [s]") 
plot(p)

ggsave("ICPE_10K.pdf", width = 10, height = 6, device = "pdf")