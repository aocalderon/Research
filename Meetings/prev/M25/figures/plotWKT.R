library(tidyverse)
library(ggplot2)
library(sf)
library(ssh)

id = "0627"
filename = paste0("pflock",id,".tsv")
session <- ssh_connect("acald013@hn")
scp_download(session, paste0("/home/acald013/tmp/",filename), to = getwd())

data = read_sf(filename)%>% select(duration, taskId, partitionId) %>% 
  mutate(partitionId = as.numeric(partitionId)) %>%
  mutate(taskId = as.numeric(taskId)) %>%
  mutate(duration = as.numeric(duration))

ggplot(data = data) +
  geom_sf(aes(fill = duration)) +
  scale_fill_viridis_c(option = "plasma") + 
  xlab("Longitude") + ylab("Latitude") +
  ggtitle("LA", subtitle = paste0("(", length(unique(data$partitionId)), " cells)"))

ggsave("map.pdf", width = 12, height = 16, dpi = "screen")
st_write(data, dsn = "cells.gpkg", layer = "cells", driver = "GPKG", append = FALSE)
ssh_disconnect(session)
