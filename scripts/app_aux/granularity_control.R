# granularity control
library(lubridate)

gran_daily <- function(dataset, input){
  #browser()
  
  input <- reactiveValuesToList(input)
  
  #debug
  #input$date <- c(as.Date("2025-01-01"), as.Date("2025-01-30"))
  
  var_name <- input$variable
  var <- rlang::sym(var_name)
  
  df <- dataset %>%
    dplyr::filter(
      region == input$region,
      uf == input$uf,
      station == input$station,
      date >= input$date[[1]],
      date <= input$date[[2]]
    ) %>% 
    dplyr::select(date, hour, !!var) %>%
    dplyr::collect() %>% 
    dplyr::group_by(date) %>%
    dplyr::summarise(
      !!paste0(var_name,"_",input$granularity) := mean(!!var, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(date)
  
  return(df)
}


gran_hourly <- function(dataset, input){
  #browser()
  
  input <- reactiveValuesToList(input)
  
  #debug
  #input$date <- c(as.Date("2025-01-01"), as.Date("2025-01-30"))
   
  var_name <- input$variable
  var <- rlang::sym(var_name)
  
  df <- dataset %>%
    dplyr::filter(
      region == input$region,
      uf == input$uf,
      station == input$station,
      date >= input$date[[1]],
      date <= input$date[[2]]
    ) %>% 
    dplyr::select(date, hour, !!var) %>%
    dplyr::collect() %>% 
    dplyr::mutate(
      hour = as.character(hour),
      hour = gsub("'|\"", ":", hour),  # caso venha no formato estranho
      datetime = lubridate::ymd(date) + lubridate::hms(hour)
    ) %>%
    dplyr::select(datetime, !!var_name) %>%
    dplyr::arrange(datetime)
  
  return(df)
}


gran_weekly <- function(dataset, input){
  input <- reactiveValuesToList(input)
  
  #debug
  #input$date <- c(as.Date("2025-01-01"), as.Date("2025-04-30"))
  
  var_name <- input$variable
  var <- rlang::sym(var_name)
  start_date <- input$date[[1]]
  
  df <- dataset %>%
    dplyr::filter(
      date >= input$date[[1]],
      date <= input$date[[2]]
    ) %>%
    dplyr::collect() %>%
    dplyr::mutate(
      week_id = floor(as.numeric(date - start_date) / 7),
      week_start = start_date + week_id * 7
    ) %>%
    dplyr::group_by(week_start) %>%
    dplyr::summarise(
      !!paste0(var_name,"_",input$granularity) := mean(!!var, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(week_start)
  
  return(df)
}

gran_monthly <- function(dataset, input){
  input <- reactiveValuesToList(input)
  
  #debug
  #input$date <- c(as.Date("2025-01-01"), as.Date("2025-04-30"))
  
  var_name <- input$variable
  var <- rlang::sym(var_name)
  start_date <- input$date[[1]]
  
  df <- dataset %>%
    dplyr::filter(
      date >= input$date[[1]],
      date <= input$date[[2]]
    ) %>%
    dplyr::collect() %>%
    dplyr::mutate(
      month_id = floor(as.numeric(date - start_date) / 30),
      month_start = start_date + month_id * 30
    ) %>%
    dplyr::group_by(month_start) %>%
    dplyr::summarise(
      !!paste0(var_name,"_",input$granularity) := mean(!!var, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(month_start)
  
  return(df)
}