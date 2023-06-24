library(tidyverse)
library(latex2exp)
library(xtable)

CACHE=FALSE
W = 6
H = 4

if(CACHE){
  read_lines( pipe( 'ssh acald013@hn "cat /home/acald013/Research/Scripts/Scala/Cliques/nohup.out"' )) |>
  write_lines("nohup.out")
} 

fields = c("ts","start","tag", "cellId", "appId","partitions","dataset","epsilon","mu","delta","project","candidateId","maximals","stage","time")
data0 = enframe(read_lines( "nohup.out" ), value = "line") |>
  filter(str_detect(line, 'MAXIMALS')) |>  
  filter(str_detect(line, 'TIME')) |>  
  separate(col = line, into = fields, sep = "\\|") |>
  select(cellId, stage, time) |>
  mutate(time = as.numeric(time), cellId = as.numeric(cellId)) |>
  filter(str_trim(stage) != "finalI") |>
  filter(str_trim(stage) != "firstM") 

data1 = data0 |> group_by(cellId, stage) |> summarise(time = sum(time))
p = ggplot(data1, aes(x = as.factor(cellId), y = time, fill = stage)) +
  geom_col() +
  scale_fill_discrete("Case", labels = c(" C d M"  = TeX("C \\neq M"),
                                          " C e M"  = TeX("C \\equiv M"),
                                          " C s M"  = TeX("C \\subset M"),
                                          " M s C"  = TeX("M \\subset C"),
                                          "Search" = "Search"
  )) +
  labs(x="Cell ID", y="Time(s)") +
  theme_bw()
plot(p)  

ggsave(paste0("performanceBySum1.pdf"), width = W, height = H)
p = ggplot(data1, aes(x = as.factor(cellId), y = time, fill = stage)) +
  geom_col(position="fill") +
  scale_fill_discrete("Case", labels = c(" C d M"  = TeX("C \\neq M"),
                                         " C e M"  = TeX("C \\equiv M"),
                                         " C s M"  = TeX("C \\subset M"),
                                         " M s C"  = TeX("M \\subset C"),
                                         "Search" = "Search"
  )) +
  labs(x="Cell ID", y="Time(%)") +
  theme_bw()
plot(p)  
ggsave(paste0("performanceBySum2.pdf"), width = W, height = H)

################################################################################

data2 = data0 |> group_by(cellId, stage) |> summarise(time = mean(time)) 
p = ggplot(data2, aes(x = as.factor(cellId), y = time, fill = stage)) +
  geom_col() +
  scale_fill_discrete("Case", labels = c(" C d M"  = TeX("C \\neq M"),
                                          " C e M"  = TeX("C \\equiv M"),
                                          " C s M"  = TeX("C \\subset M"),
                                          " M s C"  = TeX("M \\subset C"),
                                          "Search" = "Search"
  )) +
  labs(x="Cell ID", y="Time(s)") +
  theme_bw()
plot(p)  
ggsave(paste0("performanceByMean1.pdf"), width = W, height = H)

p = ggplot(data2, aes(x = as.factor(cellId), y = time, fill = stage)) +
  geom_col(position = "fill") +
  scale_fill_discrete("Case", labels = c(" C d M"  = TeX("C \\neq M"),
                                         " C e M"  = TeX("C \\equiv M"),
                                         " C s M"  = TeX("C \\subset M"),
                                         " M s C"  = TeX("M \\subset C"),
                                         "Search" = "Search"
  )) +
  labs(x="Cell ID", y="Time(%)") +
  theme_bw()
plot(p)  
ggsave(paste0("performanceByMean2.pdf"), width = W, height = H)

################################################################################

fields = c("ts","start","tag", "cellId", "appId","partitions","dataset","epsilon","mu","delta","project","candidateId","maximals","key","value")
data0 = enframe(read_lines( "nohup.out" ), value = "line") |>
  filter(str_detect(line, 'MAXIMALS')) |>  
  filter(str_detect(line, 'INFO')) |>  
  separate(col = line, into = fields, sep = "\\|") |> 
  select(cellId, candidateId, key, value) |>
  mutate(value = as.numeric(value), cellId = as.numeric(cellId),
         candidateId = as.numeric(candidateId), key = as.factor(str_trim(key))) |>
  group_by(cellId, key) |> summarise(value = mean(value)) |>
  filter(key != "finalM") |>
  pivot_wider(names_from = key, values_from = value)

data1 = tibble(cellId = c(0,1,2,3,4,5), candidates = c(10393,9579,11994,10765,7974,7936)) |> 
  left_join(data0, by = "cellId") |>
  select(cellId, candidates, sizeM, sizeH, hitPos) |>
  rename(Cell = "cellId",
         `Number candidates` = "candidates",
         `Tree size` = "sizeM",
         `Range size` = "sizeH",
         `Position found` = "hitPos"
  )
  
print(xtable(data1, digits = c(0,0,0,0,0,0)), include.rownames = F, booktabs = T)
