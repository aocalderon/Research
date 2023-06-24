library(plotly)
library(tidyverse)

data = read_tsv("~/tmp/test3d.tsv", col_names = c("x", "y", "z", "id")) |> mutate(id = as.factor(id))

fig <- plot_ly(data, x = ~x, y = ~y, z = ~z, color = ~id)
fig <- fig %>% add_markers()
fig <- fig %>% layout(scene = list(xaxis = list(title = 'x'),
                                   yaxis = list(title = 'y'),
                                   zaxis = list(title = 'z')))

fig
