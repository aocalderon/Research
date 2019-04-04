parseParams <- function(data){
  command = as.character(data %>% slice(1) %>% unlist(., use.names=FALSE))
  params = str_split(command, " --")
  n = length(params[[1]])
  params = params[[1]][3:n]
  n = length(params)
  keys = rep("", n)
  values = rep("", n)
  for(i in 1:n){
    keys[i]   = str_split(params[i], " ")[[1]][1]
    values[i] = str_split(params[i], " ")[[1]][2]
  }
  names(values) = keys 
  return(values)
}