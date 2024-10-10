library(tidyverse)
library(latex2exp)

bfe1 <- read_tsv("cmbc_bfe.tsv") |> filter(epsilon == 20) |> 
  filter(Variant == "COLLECT" | Variant == "EACH" | Variant == "HASH_3"  )
bfe2 <- read_tsv("bfe.tsv") |> filter(Stage == "Maximals" & epsilon == 20) |>  
  mutate(Variant = "BFE") |> select(Variant, epsilon, time) 
bfe <- bfe1 |> bind_rows(bfe2)

p = ggplot(data = bfe, aes(x = factor(epsilon), y = time, fill = Variant)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
  labs(x=TeX("$\\epsilon$(m)"), y="Time(s)") +
  guides(fill = guide_legend(override.aes = list(shape = NA))) +
  theme_bw()  
plot(p)

W = 6
H = 4
ggsave(paste0("cmbc_bfe.pdf"), width = W, height = H)

psi1 <- read_tsv("cmbc_psi.tsv") |> filter(epsilon == 20) |>
  filter(Variant == "COLLECT" | Variant == "EACH" | Variant == "HASH_3"  )
psi2 <- read_tsv("psi.tsv") |> filter(Stage == "Maximals" & epsilon == 20) |>  
  mutate(Variant = "PSI") |> select(Variant, epsilon, time) 
psi <- psi1 |> bind_rows(psi2)

q = ggplot(data = psi, aes(x = factor(epsilon), y = time, fill = Variant)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) +
  labs(x=TeX("$\\epsilon$(m)"), y="Time(s)") +
  guides(fill = guide_legend(override.aes = list(shape = NA))) +
  theme_bw()  
plot(q)
ggsave(paste0("cmbc_psi.pdf"), width = W, height = H)
