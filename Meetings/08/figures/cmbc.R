library(tidyverse)
library(latex2exp)

cmbc <- read_tsv("cmbc.tsv")  
p = ggplot(data = cmbc, aes(x = factor(epsilon), y = time, fill = Variant)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  labs(x=TeX("$\\epsilon$(m)"), y="Time(s)")
plot(p)