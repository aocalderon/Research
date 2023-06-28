library(tidyverse)

FILENAME="density_performance.log"
if(!file.exists(FILENAME)){
  read_lines( pipe(paste0('ssh acald013@hn "cat /home/acald013/Research/Scripts/Scala/PFlock/', FILENAME, '"')) ) |>
    write_lines(FILENAME)
}

fieldsArgs = c("ts","start","tag","appId","arg","value")
args = enframe(read_lines(FILENAME), value = "line") |>
  filter(str_detect(line, 'PARAMS')) |>  
  separate(col = line, into = fieldsArgs, sep = "\\|") |>
  select(appId, arg, value) |>
  filter(arg == "density") |>
  pivot_wider(names_from = arg, values_from = value) |>
  mutate(density = recode(density, "0" = "NA", "8000" = ">8000"))
  
fieldsTime = c("ts","start","host","tag","cellId","appId","partitions","dataset","epsilon","mu","delta","project","stage","time")
times = enframe(read_lines(FILENAME), value = "line") |>
  filter(str_detect(line, 'TIME')) |>  
  separate(col = line, into = fieldsTime, sep = "\\|") |>
  select(appId, stage, time) |>
  mutate(time = as.numeric(time), stage = as.factor(stage)) 

data = times |> left_join(args, by = "appId")

dataDensity = data |> filter(stage != "Read") |> 
  group_by(density, stage) |> summarise(time = mean(time)) |>
  group_by(density) |> summarise(time = sum(time))

p = ggplot(dataDensity, aes(x = as.factor(density), y = time)) +
  geom_col(position = "dodge") +
  labs(x="Dense approach", y="Time(s)") +
  theme_bw()
plot(p)  

dataDensityStage = data |> filter(stage != "Read") |> 
  group_by(density, stage) |> summarise(time = mean(time))
  
p = ggplot(dataDensityStage, aes(x = as.factor(density), y = time, fill = as.factor(stage))) +
  geom_col(position = "dodge") +
  labs(x="Dense approach", y="Time(s)") +
  theme_bw()
plot(p)  

#W = 8
#H = 4
#ggsave(paste0("performanceByCellStage.pdf"), width = W, height = H)
