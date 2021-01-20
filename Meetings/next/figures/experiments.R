require(tidyverse)

log = enframe(readLines("experiments.txt"))
data0 = log %>% separate(value, into = c("min", "value"), sep = "m") %>% 
  separate(value, into = c("sec", NA), sep = "s") %>%
  mutate(time = as.numeric(min) * 60 + as.numeric(sec))
data0$method = c(rep("Cliques", 5), rep("Control", 5))

data = data0 %>% group_by(method) %>% summarise(time = mean(time))

p = ggplot(data = data, aes(x = method, y = time)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Method", y="Time [sec]", title="Performance") 
plot(p)


ggsave(paste0("experiments.pdf"), device = "pdf")