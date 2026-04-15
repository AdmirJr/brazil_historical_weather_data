library(shiny)
library(arrow)
library(dplyr)
library(ggplot2)
library(stringr)

# UI: Interface do Usuário
ui <- fluidPage(
  theme = bslib::bs_theme(version = 5, bootswatch = "minty"),
  titlePanel(NULL),
  
  sidebarLayout(
    sidebarPanel(
      selectInput(
        inputId = "test_change",
        label = "test_change",
        choices = c("test1","test2")
      ),
      selectInput(
        inputId = "region",
        label = "Region",
        choices = c("S")
      ),
      selectInput(
        inputId = "uf",
        label = "UF",
        choices = c("PR")
      ),
      selectInput(
        inputId = "station",
        label = "Station",
        choices = c("FOZ DO IGUACU"),
      ),
      dateRangeInput(
        inputId = "date",
        label = "Date",
        start = Sys.Date() - 15,
        end = Sys.Date()),
      selectInput("variable", "Variable", 
                  choices = c(
                    "atm_pressure (mB)"
                  )),
      selectInput(
        inputId = "granularity",
        label = "Granularity",
        choices = c("Hourly","Daily","Weekly","Monthly"),
      ),
      
      downloadButton("downloadData","Download data"),
      
      hr(),
      helpText(
        a("by Admir Junior",
          href = "https://admirjr.github.io/",
          target="_blank"))
    ),
    
    mainPanel(
      plotOutput("lineChart", height = "500px"),
      tableOutput("summaryTable")
    )
  )
)

server <- function(input, output, session) {
  dataset <- arrow::open_dataset("data/processed_01/") 
  
  observeEvent(
    {
      input$variable
      input$station
      input$uf
      input$region
    },
    {
      lad <- last_day_available(dataset, input)
      
      updateDateRangeInput(
        session,
        inputId = "date",
        start = lad - 7,
        end = lad
      )
    },
    ignoreInit = FALSE
  )
  
  # 1. Filtro Reativo Granular
  filtered_data <- reactive({
    source("scripts/app_aux/granularity_control.R")
    source("scripts/app_aux/last_day_available.R")
    
    shiny::req(input$variable, input$date, input$station, input$uf, input$region, input$granularity)
    
    print(input$granularity)
    
    df <- switch (input$granularity,
      "Hourly" = gran_hourly(dataset, input),
      "Daily" = gran_daily(dataset, input),
      "Weekly" = gran_weekly(dataset, input),
      "Monthly" = gran_monthly(dataset, input)
    )
    
    
    # df <- dataset %>%
    #   dplyr::filter(
    #     region == input$region,
    #     uf == input$uf,
    #     station == input$station,
    #     date >= input$date[[1]],
    #     date <= input$date[[2]]) %>% 
    #   dplyr::select(date, hour, !!rlang::sym(input$variable)) %>%
    #   dplyr::collect() %>% 
    #   dplyr::arrange(hour)
    # 
    
    if (input$granularity != "Hourly") {
      var_plot <- paste0(input$variable, "_", input$granularity)
    } else {
      var_plot <- input$variable
    }
    
    x_var <- if (input$granularity == "Daily") "date" else "datetime"
    x_var <- switch (input$granularity,
      "Hourly" = "datetime",
      "Daily" = "date",
      "Weekly" = "week_start",
      "Monthly" = "month_start"
    )
    
    p <- ggplot2::ggplot(df, ggplot2::aes(x = .data[[x_var]], y = .data[[var_plot]])) +
      ggplot2::geom_line(color = "#e67e22", linewidth = 1) +
      ggplot2::geom_point(color = "#d35400") + 
      ggplot2::theme_minimal(base_size = 14) +
      ggplot2::labs(
        title = paste0(input$granularity,": ", input$variable),
        subtitle = paste(input$day, "/", input$month, "/", input$year),
        x = stringr::str_to_sentence(x_var) %>% str_replace("_"," "),
        y = input$variable
      )
    
    p <- ggplot2::ggplot(df, ggplot2::aes(x = .data[[x_var]], y = .data[[var_plot]])) +
      ggplot2::geom_line(color = "#e67e22", linewidth = 1) +
      ggplot2::geom_point(color = "#d35400") + 
      ggplot2::theme_minimal(base_size = 14) +
      ggplot2::labs(
        title = paste0(input$granularity, ": ", input$variable),
        subtitle = paste(input$day, "/", input$month, "/", input$year),
        x = stringr::str_to_sentence(x_var) %>% stringr::str_replace("_"," "),
        y = input$variable
      ) +
      ggplot2::scale_x_datetime(breaks = scales::pretty_breaks(n = 10)) +
      ggplot2::scale_y_continuous(breaks = scales::pretty_breaks(n = 10)) +
      theme(
        axis.text.x = element_text(angle = 45, hjust = 1)
      )
    
    p
  })
  
  output$downloadData <- downloadHandler(
    filename = function() {
      # Gera um nome dinâmico baseado no filtro
      paste0("inmet_", input$date, ".csv")
    },
    content = function(file) {
      # Obtém os dados que já foram filtrados pelo seu reativo 'filtered_data'
      data_to_save <- filtered_data()
      
      # Escreve o arquivo para o usuário
      readr::write_csv(data_to_save, file)
    }
  )
  
  # 2. Gráfico por Hora
  output$lineChart <- renderPlot({
    df <- filtered_data()
    plot(df)
    # Validação para não quebrar o app se o dia não existir (ex: 31 de fevereiro)
    # shiny::validate(
    #   need(nrow(df) > 0, "Nenhum dado encontrado para esta data específica.")
    # )
    # 
    # ggplot2::ggplot(df, ggplot2::aes(x = hour, y = .data[[input$variable]])) +
    #   ggplot2::geom_line(color = "#e67e22", linewidth = 1) +
    #   ggplot2::geom_point(color = "#d35400") + # Pontos ajudam a ver as medições horárias
    #   ggplot2::theme_minimal(base_size = 14) +
    #   ggplot2::labs(
    #     title = paste("Hourly variation:", input$variable),
    #     subtitle = paste(input$day, "/", input$month, "/", input$year),
    #     x = "Time",
    #     y = input$variable
    #   )
  })
}

shinyApp(ui, server)
