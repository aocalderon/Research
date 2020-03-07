require(tidyverse)

data = read_tsv("ByIndexer4.tsv")

p = ggplot(data = data, aes(x = Phase, y = Time, fill = Index)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
  labs(x="Phase", y="Time [s]", title="Execution time initial phases in MF by index type") 
plot(p)
ggsave("ByIndexer4.pdf", width = 8, height = 7, device = "pdf")
