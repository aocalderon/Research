library(tidyverse)
library(latex2exp)

cmbc <- read_tsv("cmbc_psi.tsv")
psi  <- read_tsv("psi.tsv") |> filter(Stage == "Maximals" & epsilon == 20) |> select(epsilon, time) |> mutate(Method = "PSI")
p = ggplot(data = cmbc, aes(x = factor(epsilon), y = time, fill = Variant)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  geom_point(data = psi, aes(group = Method, fill = NULL, color = Method), size = 1.25) +
  labs(x=TeX("$\\epsilon$(m)"), y="Time(s)") +
  guides(fill = guide_legend(override.aes = list(shape = NA))) +
  theme_bw()  
plot(p)

W = 8
H = 6
ggsave(paste0("cmbc_variants_psi.pdf"), width = W, height = H)