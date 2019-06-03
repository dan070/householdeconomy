#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library(shiny)
#library(RSQLite)
library(DBI)
library(rhandsontable)
#print(getwd())





# Define UI for application that draws a histogram
ui <- fluidPage(
   
   # Application title
   titlePanel("Unlabeled data from database table _unlabeled_"),
   
   # Sidebar with a slider input for number of bins 
   sidebarLayout(
      sidebarPanel(
        shiny::actionButton(inputId = "actionbutton_1", label = "Save"),
        shiny::actionButton(inputId = "actionbutton_1", label = "Finish")
        
        
        # put something here
      ),
      
      # Show a plot of the generated distribution
      mainPanel(
         rhandsontable::rHandsontableOutput(outputId = "rhot1")
      )
   )
)


# Define server logic required to draw a histogram
server <- function(input, output) {
  
  
  output$rhot1 <- renderRHandsontable(expr = {
    print("render run")
    rhandsontable::rhandsontable(data = reactive_unlabeled() ) %>% 
      rhandsontable::hot_col(col = "manual_label", 
                             type = "dropdown",
                             source = accepted_labels,
                             strict = F,
                             allowInvalid = F)
  })
  
  reactive_unlabeled <- reactive(x = {
    #return(cars)
    # print("reactive run")
    # return(dbGetQuery(mydb, "select * from unlabeled"))
    print(paste("Save pressed:", input$actionbutton_1))
    if(input$actionbutton_1 == 0) {
      return(getdata())
      } else {
        inp <- hot_to_r(input$rhot1)
        print(inp[1:5, ])
      }
    
  })
  

    
  
  
}

# Run the application 
shinyApp(ui = ui, server = server)

