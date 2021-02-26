require(tidyverse)

data = read_csv("data.csv", col_names = c("approach", "start", "end")) %>% 
  mutate(time = as.numeric(end)/1000.0 - as.numeric(start)/1000.0)

p = ggplot(data = data, aes(x = approach, y = time)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Approach", y="Time [sec]", title="Performance") 
plot(p)

ggsave(paste0("data.pdf"), device = "pdf")