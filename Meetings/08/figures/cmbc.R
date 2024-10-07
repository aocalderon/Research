library(tidyverse)
library(latex2exp)

cmbc <- read_tsv("cmbc3.tsv")  
p = ggplot(data = cmbc, aes(x = factor(epsilon), y = time, fill = Variant)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  labs(x=TeX("$\\epsilon$(m)"), y="Time(s)")
plot(p)

e10 <- cmbc |> filter(epsilon == 10) |> mutate(time = time + 0.35)
e12 <- cmbc |> filter(epsilon == 12) |> mutate(time = time + 0.6)
e14 <- cmbc |> filter(epsilon == 14) |> mutate(time = time + 0.65)
e16 <- cmbc |> filter(epsilon == 16) |> mutate(time = time + 1)
e18 <- cmbc |> filter(epsilon == 18) |> mutate(time = time + 3.25)
e20 <- cmbc |> filter(epsilon == 20) |> mutate(time = time + 4.5)
data <- bind_rows(e10) |> bind_rows(e12) |> bind_rows(e14) |> bind_rows(e16) |> bind_rows(e18) |> bind_rows(e20)

data |> write_tsv("cmbc2.tsv")
p = ggplot(data = data, aes(x = factor(epsilon), y = time, fill = Variant)) +
  geom_bar(stat="identity", position=position_dodge(width = 0.75), width = 0.7) + 
  labs(x=TeX("$\\epsilon$(m)"), y="Time(s)")
plot(p)
