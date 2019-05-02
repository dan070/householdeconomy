# R6 object to define any constants used throughout project.

Constants <- R6::R6Class(classname = "Constants", 
            public = list(
              get_accepted_labels = function(){
                return(private$accepted_labels)
              }
              
            ),
            private = list(
              # Predefined labels to use
              accepted_labels = c("loans,insurance,bankfee", 
                                   "restaurant,coffee", 
                                   "entertainment,vacation",
                                   "groceries",
                                   "clothes",
                                   "cash,swish",
                                   "childcare",
                                   "salary,benefits",
                                   "transportation",
                                   "household",
                                   "medical",
                                   "other,unknown"
              )
              
            )
            
            )

#myconsts <- Constants$new()

#myconsts$get_accepted_labels()

