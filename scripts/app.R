library(shiny)
library(arrow)
library(dplyr)
library(ggplot2)
library(stringr)
library(lubridate)

# UI: Interface do Usuário
ui <- fluidPage(
  theme = bslib::bs_theme(version = 5, bootswatch = "minty"),
  titlePanel(NULL),
  
  sidebarLayout(
    sidebarPanel(
      selectInput(
        inputId = "region",
        label = "Region",
        choices = c("S","SE","N","NE","CO"),
        multiple = TRUE
      ),
      selectInput(
        inputId = "uf",
        label = "UF",
        choices = NULL,
        multiple = TRUE
      ),
      selectInput(
        inputId = "station",
        label = "Station",
        choices = NULL, 
        multiple = TRUE
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
  station_data <- arrow::read_parquet("data/station_data.parquet")
  
  observeEvent(
    {
      input$region
    },
    {
      reg_states <- station_data %>%
        filter(region %in% reactiveValuesToList(input)$region) %>%
        pull(uf) %>%
        unique()
      
      updateSelectInput(
        session,
        inputId = "uf",
        choices = reg_states
      )
    }
  )
  
  observeEvent(
    {
      input$uf
    },
    {
      uf_stations <- station_data %>%
        filter(uf %in% input$uf) %>%
        pull(station) %>%
        unique()
      
      updateSelectInput(
        session,
        inputId = "station",
        choices = uf_stations
      )
    }
  )
  
  observeEvent(
    {
      input$variable
      input$station
      input$uf
      input$region
    },
    {
      lad <- last_day_available(station_data, input)
      
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
    source("scripts/app_aux/level_summ.R")
    
    shiny::req(input$variable, input$date, input$region, input$granularity)
    
    print(input$granularity)
    
    df <- switch (input$granularity,
      "Hourly" = gran_hourly(dataset, input),
      "Daily" = gran_daily(dataset, input),
      "Weekly" = gran_weekly(dataset, input),
      "Monthly" = gran_monthly(dataset, input)
    ) 
    
    #df <- summarise_level(df, input)
    
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
