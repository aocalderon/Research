library(tidyverse)
library(ggpubr)

W = 6
H = 4
psi <- read_tsv("cmbc_psi_stages.tsv") |> filter(variant != "HASH")

labels = c("Collect","Each","MBC","Cliques","Maximals","Candidates","Centers","Pairs")
q = ggplot(data = psi, aes(x = variant, y = time, fill = stage)) +
  geom_bar(stat="identity", position="stack") +
  labs(x="(b)", y="Time(s)") +
  theme_bw() + 
  scale_fill_brewer(name = "Stage", palette = "Paired", labels = labels) 
  
plot(q)
ggsave(paste0("cmbc_psi.pdf"), width = W, height = H)

bfe <- read_tsv("cmbc_bfe_stages.tsv") |> filter(variant != "HASH")
bfe_levels <- c("COLLECT", "EACH", "BFE")
p = ggplot(data = bfe, aes(x = factor(variant, level = bfe_levels), y = time, fill = stage)) +
  geom_bar(stat="identity", position="stack") +
  labs(x="(a)", y="Time(s)") +
  theme_bw() + 
  scale_fill_brewer(name = "Stage", palette = "Paired", labels = labels) 

plot(p)
ggsave(paste0("cmbc_bfe.pdf"), width = W, height = H)

ggarrange(p, q, ncol=2, nrow=1, common.legend = TRUE, legend="bottom")
ggsave(paste0("cmbc.pdf"), width = 10, height = 4)
