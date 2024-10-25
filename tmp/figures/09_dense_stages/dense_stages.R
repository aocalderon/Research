library(tidyverse)
library(latex2exp)
library(ggpubr)

bfe <- read_tsv("bfe.tsv")
p = ggplot(bfe, aes(x = as.factor(epsilon), y = time, group = Stage, color = Stage, shape = Stage, linetype = Stage)) +
  labs(title="(a)", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  geom_line() + 
  geom_point(size = 2.5) +
  theme_bw() 
plot(p)  

W = 6
H = 4
#ggsave(paste0("dense_stages_bfe.pdf"), width = W, height = H)

psi <- read_tsv("psi.tsv")
q = ggplot(psi, aes(x = as.factor(epsilon), y = time, group = Stage, color = Stage, shape = Stage, linetype = Stage)) +
  labs(title="(b)", x=TeX("$\\epsilon(m)$"), y="Time(s)") +
  geom_line() + 
  geom_point(size = 2.5) +
  theme_bw() 
plot(q)

#ggsave(paste0("dense_stages_psi.pdf"), width = W, height = H)

ggarrange(p, q, ncol=2, nrow=1, common.legend = TRUE, legend="bottom")
ggsave(paste0("dense.pdf"), width = 8, height = 4)

# data <- bfe |> mutate(method = "BFE") |> bind_rows( psi |> mutate(method = "PSI") )
# r = ggplot(data, aes(x = as.factor(epsilon), y = time, group = Stage, color = Stage, shape = Stage, linetype = Stage)) +
#   labs(x=TeX("$\\epsilon(m)$"), y="Time(s)") +
#   geom_line() + 
#   geom_point(size = 2.5) +
#   facet_wrap(~method) +
#   theme_bw() 
# plot(r)

#W = 8
#H = 4
#ggsave(paste0("dense_stages.pdf"), width = W, height = H)
