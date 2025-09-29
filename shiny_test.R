library(shiny)
library(ggplot2)
library(dplyr)

# Sample lookup for organisations by level
org_lookup <- list(
  "ICB" = c("ICB A", "ICB B", "ICB C"),
  "Local Authority" = c("LA X", "LA Y", "LA Z"),
  "PCN" = c("PCN 1", "PCN 2", "PCN 3")
)

ui <- fluidPage(
  
  titlePanel("Care-shift Tracker"),
  
  sidebarLayout(
    sidebarPanel(
      selectInput(
        inputId  = "Area Level",
        label    = "Select organisation level",
        choices  = names(org_lookup),
        selected = names(org_lookup)[1]
      ),
      uiOutput("org_ui")  # dynamic selectInput for chosen level
    ),
    
    mainPanel(
      tabsetPanel(
        id = "tabs",
        
        tabPanel(
          title = "Introduction",
          h4("Welcome and Overview"),
          p("Use this section to explain the purpose, data sources, and key takeaways."),
          plotOutput("intro_summary_chart")
        ),
        
        tabPanel(
          title = "Tracker",
          h4("Time Series Tracker"),
          # you can replace with plotlyOutput or multiple plots in a grid
          uiOutput("tracker_plots")
        ),
        
        tabPanel(
          title = "Comparison",
          h4("Quadrant Plots: Current vs Change"),
          uiOutput("comparison_plots")
        ),
        
        
        tabPanel(
          title = "Explorer",
          h4("Explore Relationships Between Indicators"),
          fluidRow(
            column(6,
                   selectInput("explorer_x", "Select X-axis Indicator", choices = NULL)
            ),
            column(6,
                   selectInput("explorer_y", "Select Y-axis Indicator", choices = NULL)
            )
          ),
          plotOutput("explorer_plot", height = "400px")
        ),
        
        tabPanel(
          title = "Metadata",
          h4("Indicator Definitions"),
          dataTableOutput("metadata_table")
        )
        
      )
    )
  )
)

server <- function(input, output, session) {
  
  # Dynamically render organisation selector based on chosen level
  output$org_ui <- renderUI({
    req(input$level)
    selectInput(
      inputId = "organisation",
      label   = paste("Choose", input$level),
      choices = org_lookup[[input$level]],
      selected = org_lookup[[input$level]][1]
    )
  })
  
  # Introduction summary chart placeholder
  output$intro_summary_chart <- renderPlot({
    # Replace with actual summarisation of your indicators
    df <- data.frame(
      indicator = paste0("Ind", 1:5),
      value     = runif(5, 50, 100)
    )
    ggplot(df, aes(x = indicator, y = value)) +
      geom_col(fill = "steelblue") +
      theme_minimal() +
      labs(x = NULL, y = "Value", title = "Summary of Key Indicators")
  })
  
  # Tracker: generate a grid of time-series charts
  output$tracker_plots <- renderUI({
    # Suppose you have a list of indicator names grouped by theme
    themes <- list(
      "Theme 1" = paste0("A", 1:3),
      "Theme 2" = paste0("B", 1:4),
      "Theme 3" = paste0("C", 1:5)
    )
    
    plot_output_list <- lapply(names(themes), function(theme) {
      plotname <- paste0("ts_", theme)
      output[[plotname]] <- renderPlot({
        # Dummy timeseries
        dates <- seq.Date(Sys.Date()-365, Sys.Date(), length.out = 12)
        df <- expand.grid(date = dates, ind = themes[[theme]])
        df$value <- runif(nrow(df), 20, 200)
        ggplot(df, aes(x = date, y = value, color = ind)) +
          geom_line() +
          theme_minimal() +
          labs(title = theme, x = NULL, y = "Value")
      })
      plotOutput(plotname, height = "250px")
    })
    
    # arrange plots in rows
    do.call(tagList, plot_output_list)
  })
  
  # Comparison: quadrant plots placeholder
  output$comparison_plots <- renderUI({
    # Assume same themes and indicators as above
    themes <- list(
      "Theme 1" = paste0("A", 1:3),
      "Theme 2" = paste0("B", 1:4),
      "Theme 3" = paste0("C", 1:5)
    )
    
    quad_list <- lapply(names(themes), function(theme) {
      plotname <- paste0("quad_", theme)
      output[[plotname]] <- renderPlot({
        df <- data.frame(
          ind       = themes[[theme]],
          current   = runif(length(themes[[theme]]), 50, 150),
          change_pct = runif(length(themes[[theme]]), -20, 20)
        )
        ggplot(df, aes(x = change_pct, y = current, label = ind)) +
          geom_hline(yintercept = median(df$current), linetype = "dashed") +
          geom_vline(xintercept = 0, linetype = "dashed") +
          geom_point(size = 3) +
          geom_text(nudge_y = 5) +
          theme_minimal() +
          labs(
            title = paste("Quadrant Plot —", theme),
            x = "Percent Change",
            y = "Current Value"
          )
      })
      plotOutput(plotname, height = "300px")
    })
    
    do.call(tagList, quad_list)
  })
  
  # Metadata definitions
  output$metadata_table <- renderDataTable({
    # Replace with your real metadata
    data.frame(
      Indicator   = paste0("Ind", 1:12),
      Definition  = paste("Definition of indicator", 1:12),
      Source      = "Internal data warehouse",
      stringsAsFactors = FALSE
    )
  })
  
  # Sample indicator list (replace with your actual list)
  all_indicators <- paste0("Ind", 1:12)
  
  # Populate dropdowns when app loads
  observe({
    updateSelectInput(session, "explorer_x", choices = all_indicators, selected = all_indicators[1])
    updateSelectInput(session, "explorer_y", choices = all_indicators, selected = all_indicators[2])
  })
  
  # Render scatterplot
  output$explorer_plot <- renderPlot({
    req(input$explorer_x, input$explorer_y)
    
    # Dummy data — replace with actual indicator values
    df <- data.frame(
      x = rnorm(100, mean = 50, sd = 10),
      y = rnorm(100, mean = 50, sd = 10)
    )
    
    # Linear model
    fit <- lm(y ~ x, data = df)
    r2 <- summary(fit)$r.squared
    corr <- cor(df$x, df$y)
    
    ggplot(df, aes(x = x, y = y)) +
      geom_point(alpha = 0.7, color = "darkblue") +
      geom_smooth(method = "lm", se = FALSE, color = "red") +
      theme_minimal() +
      labs(
        x = input$explorer_x,
        y = input$explorer_y,
        title = paste("Scatterplot of", input$explorer_x, "vs", input$explorer_y),
        subtitle = paste0("Correlation: ", round(corr, 2), 
                          " | R²: ", round(r2, 2))
      )
  })
  
}

shinyApp(ui, server)