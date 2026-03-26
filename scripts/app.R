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
      dateInput(
        inputId = "date",
        label = "Date",
        value = "2025-01-01"),
      
      selectInput("variable", "Variable", 
                  choices = c(
                    "atm_pressure (mB)"
                  )),
      
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
  
  
  
  # 1. Filtro Reativo Granular
  filtered_data <- reactive({
    shiny::req(input$variable, input$date, input$station, input$uf, input$region)
    
    df <- dataset %>%
      dplyr::filter(
        region == input$region,
        uf == input$uf,
        station == input$station,
        date == input$date) %>% 
      dplyr::select(date, hour, !!rlang::sym(input$variable)) %>%
      dplyr::collect() %>% 
      dplyr::arrange(hour)
    
    print(head(df))
    
    df
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
    
    # Validação para não quebrar o app se o dia não existir (ex: 31 de fevereiro)
    shiny::validate(
      need(nrow(df) > 0, "Nenhum dado encontrado para esta data específica.")
    )
    
    ggplot2::ggplot(df, ggplot2::aes(x = hour, y = .data[[input$variable]])) +
      ggplot2::geom_line(color = "#e67e22", linewidth = 1) +
      ggplot2::geom_point(color = "#d35400") + # Pontos ajudam a ver as medições horárias
      ggplot2::theme_minimal(base_size = 14) +
      ggplot2::labs(
        title = paste("Hourly variation:", input$variable),
        subtitle = paste(input$day, "/", input$month, "/", input$year),
        x = "Time",
        y = input$variable
      )
  })
}

shinyApp(ui, server)
