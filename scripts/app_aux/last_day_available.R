# last_day_avaible
last_day_available <- function(dataset, input){
  input <- reactiveValuesToList(input)
  var_name <- input$variable
  var <- rlang::sym(var_name)
  
  lad <- dataset %>%
    dplyr::filter(
      region == input$region,
      uf == input$uf,
      station == input$station,
      !is.na(!!var)
    ) %>%
    dplyr::slice_max(date, n = 1, with_ties = FALSE) %>%
    dplyr::collect() %>%
    dplyr::pull(date)
  
  return(as.Date(lad))
}