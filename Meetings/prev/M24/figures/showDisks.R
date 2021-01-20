require(tidyverse)

disks = read_tsv("disks.wkt", col_names = c("wkt", "pids", "partitionId", "taskId"))
