# last_day_avaible
last_day_available <- function(station_data, input){
  input <- reactiveValuesToList(input)
  
  var_name <- paste0("lad_",input$variable)
  
  not_null_scopus <- names(Filter(Negate(is.null), input[c("region","uf","station")]))
  
  deep_level <- length(not_null_scopus) %>% as.character()
  
  lad <- switch (deep_level,
    "3" = {
      station_data %>%
        select(region, uf, station, !!var_name) %>%
        filter(
          region %in% input$region,
          (is.null(input$uf) | uf %in% input$uf),
          (is.null(input$station) | station %in% input$station),
          !is.na(!!sym(var_name))) %>%
        dplyr::slice_max(!!sym(var_name), n = 1, with_ties = FALSE) %>%
        pull(!!sym(var_name))
    },
    "2" = {
      station_data %>%
        select(all_of(not_null_scopus), !!var_name) %>%
        filter(
          region %in% input$region,
          (is.null(input$uf) | uf %in% input$uf),
          !is.na(!!sym(var_name))) %>%
        dplyr::slice_max(!!sym(var_name), n = 1, with_ties = FALSE) %>%
        pull(!!sym(var_name))
    },
    "1" = {
      station_data %>%
        select(all_of(not_null_scopus), !!var_name) %>%
        filter(
          region %in% input$region,
          !is.na(!!sym(var_name))) %>%
        dplyr::slice_max(!!sym(var_name), n = 1, with_ties = FALSE) %>%
        pull(!!sym(var_name))
    }
  )
  
  return(lad)
}
# last_day_available <- function(dataset, input){
#   input <- reactiveValuesToList(input)
#   var_name <- input$variable
#   var <- rlang::sym(var_name)
#   
#   browser()
#   
#   lad <- dataset %>%
#     dplyr::filter(
#       region %in% input$region,
#       (is.null(input$uf) | uf %in% input$uf),
#       (is.null(input$station) | station %in% input$station),
#       !is.na(!!var)
#     ) %>%
#     dplyr::slice_max(date, n = 1, with_ties = FALSE) %>%
#     dplyr::collect() %>%
#     dplyr::pull(date)
#   
#   return(as.Date(lad))
# }
# 
# last_day_available_alt <- function(dataset, inputs){
#   available_years <- list.files("data/processed_01") %>% str_remove("_data.parquet")
#   
#   sapply(available_years, function(y){
#     read_parquet(str_glue("data/processed_01/{y}_data.parquet")) %>%
#       dplyr::filter(
#         region %in% input$region,
#         (is.null(input$uf) | uf %in% input$uf),
#         (is.null(input$station) | station %in% input$station),
#         !is.na(!!var)
#       ) %>%
#       dplyr::slice_max(date, n = 1, with_ties = FALSE) %>%
#       dplyr::collect() %>%
#       dplyr::pull(date)
#   })
#   
#     
# }