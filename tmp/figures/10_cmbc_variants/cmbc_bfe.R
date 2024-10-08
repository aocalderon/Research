library(tidyverse)
library(latex2exp)

cmbc <- read_tsv("cmbc_bfe.tsv")
bfe  <- read_tsv("bfe.tsv") |> filter(Stage == "Maximals") |> select(epsilon, time) |> mutate(Method = "BFE")
p = ggplot(data = cmbc, aes(x = factor(epsilon), y = time, fill = Variant)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  geom_line(data = bfe, aes(group = Method, fill = NULL, color=Method)) +
  geom_point(data = bfe, aes(group = Method, fill = NULL, color = Method), size = 1.25) +
  labs(x=TeX("$\\epsilon$(m)"), y="Time(s)") +
  guides(fill = guide_legend(override.aes = list(shape = NA))) +
  theme_bw()  
plot(p)

W = 8
H = 6
ggsave(paste0("cmbc_variants_bfe.pdf"), width = W, height = H)