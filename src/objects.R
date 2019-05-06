






# Objects used to simplify handling of data to and fro the database.


SmallTableObject <-
  R6::R6Class(
    classname = "SmallTableObject",
    
    #~~~~~~~~~~~~~~~~~~~~~~~~
    # ~ Public ~
    #~~~~~~~~~~~~~~~~~~~~~~~~
    public = list(
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Subset ~
      #~~~~~~~~~~~~~~~~~~~~~~~~
      subset = function(x = NULL,
                        y = NULL,
                        value = NULL) {
        print("subset function called.")
        print(x)
        print(y)
        print(value)
        #if value is null, we return the frame (possibly subsetted)
        if (is.null(x) && is.null(y) && is.null(value)) {
          print("1st if statement.")
          return(private$table_df)
        }
        if (!is.null(x) && !is.null(y) && is.null(value)) {
          print("2nd if statement.")
          return(private$table_df[x, y])
        }
        if (is.null(x) && !is.null(y) && is.null(value)) {
          print("3nd if statement.")
          return(private$table_df[, y])
        }
        if (!is.null(x) && is.null(y) && is.null(value)) {
          print("4th if statement.")
          return(private$table_df[x,])
        }
        
        # If value is not null,  we need to check the value.
        if (is.null(x) && is.null(y) && !is.null(value)) {
          # If x and y null, then [,] <- whole.data.frame
          print("5th if statement.")
          assertcollection <- checkmate::makeAssertCollection()
          # Check : value is a data frame.
          # Check : right number of columns.
          # Check : each column class.
          checkmate::assert_data_frame(
            x = value,
            types = private$table_types,
            ncols = length(private$table_df),
            add = assertcollection
          )
          # Check : column names.
          checkmate::assert_names(
            x = value,
            identical.to = names(private$table_df),
            add = assertcollection
          )
          
          # If we pass then just put data in.
          # Return self.
          private$table_df <- value
          return(self)
        }
        if (!is.null(x) && !is.null(y) && !is.null(value)) {
          print("6th if statement.")
          # We are not allowed to change the configuration of the data frame.
          # The classes have to be in line with what is already there.
          # And they have to be valid identifiers for that data frame.
          #
          # Make a copy of the table df. 
          # Assign to it.
          # Check verification of copy.
          # Swap the copy into the private field.
          tmp_table_df <- private$table_df
          
          tmp_table_df[x, y] <- value
          
          private$table_types
          
          TODO : check the table types on this assignment.
          if table types are unchanged, then we are cool.
          Any other weird assignment will be error by default in R.
          
          lapply(tmp_table_df, class)
          
          tmp_class_value = apply(X = value,
                                  MARGIN = 2,
                                  FUN = class)
          tmp_class_df = apply(X = private$table_df[, y],
                               MARGIN = 2,
                               FUN = class)
          checkmate::assert_set_equal(
            x = tmp_class_df,
            y = tmp_class_value,
            ordered = T,
            add = assertcollection
          )
          private$table_df[x, y] <- value
          
          return(self)
        }
        if (is.null(x) && !is.null(y) && !is.null(value)) {
          print("7th if statement.")
          return(private$table_df[, y])
        }
        if (!is.null(x) && is.null(y) && !is.null(value)) {
          print("8th if statement.")
          return(private$table_df[x,])
        }
        
        
        
        # LEGACY CODE DOWN HERE TO USE LATER
        # # compare the form of input df to the object private df.
        # assertcollection <- checkmate::makeAssertCollection()
        # # Check for number of cols and types.
        # checkmate::assertDataFrame(
        #   x = df,
        #   types = private$table_types,
        #   ncols = length(private$table_df),
        #   add = assertcollection
        # )
        # # Check names of columns.
        # checkmate::assertSetEqual(names(df),
        #                           names(private$table_df),
        #                           ordered = T,
        #                           add = assertcollection)
        #
        # # Check table integrity (ensure no data race on local table versus target table).
        # tryCatch(
        #   expr = {
        #     # Get target table (again)
        #     table_verification <-
        #       RSQLite::dbReadTable(conn = private$connection, name = private$tablename)
        #     # Check classes
        #     checkmate::assertDataFrame(x = private$table_df,
        #                                types = unlist(lapply(table_verification, class)),
        #                                add = assertcollection)
        #     # Check names
        #     checkmate::assertSetEqual(
        #       names(private$table_df),
        #       names(table_verification),
        #       ordered = T,
        #       add = assertcollection
        #     )
        #
        #   },
        #   error = function(e) {
        #     assertcollection$push("Could not read the table on data base side, while checking for data integrity.")
        #   },
        #   finally = {
        #   }
        # )
        
        # # Do hashing of the columns, and compare.
        # hash_1 <- private$hash_data_frame(table_verification)
        # hash_2 <- private$table_hash
        # if(hash_1 != hash_2)
        #   assertcollection$push("")
        
      },
      
      
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Print ~
      #~~~~~~~~~~~~~~~~~~~~~~~~
      print = function(...) {
        print("SmallTableObject")
        print(paste("db type : ", private$dbtype))
        print(paste("table name : ", private$tablename))
        invisible(self)
      },
      
      
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Initialize ~
      #~~~~~~~~~~~~~~~~~~~~~~~~
      initialize = function(dbtype = NULL,
                            host = NULL,
                            db = NULL,
                            user = NULL,
                            pass = NULL,
                            tablename = NULL) {
        assertcollection <- checkmate::makeAssertCollection()
        checkmate::assertChoice(x = dbtype,
                                choices = c("sqlite"),
                                add = assertcollection)
        
        # SQLITE specific
        if (dbtype == "sqlite") {
          # Private field "dbtype" updated.
          private$dbtype <- dbtype
          
          # Check string input types.
          checkmate::assert_character(
            x = host,
            len = 1,
            min.chars = 1,
            add = assertcollection
          )
          checkmate::assert_character(
            x = tablename,
            len = 1,
            min.chars = 1,
            add = assertcollection
          )
          # Private field "tablename" updated.
          private$tablename <- tablename
          
          # Check host points to valid sqlite database file.
          if (!checkmate::testFileExists(x = host))
            assertcollection$push("Sqlite requires HOST argument to be local file.")
          checkmate::assertFileExists(x = host, add = assertcollection)
          
          # Private field "host" updated.
          private$host <- host
          
          # Check file connection is possible.
          tryCatch({
            rsqliteconnection <-
              RSQLite::dbConnect(drv = RSQLite::SQLite(), dbname = host)
            
            # Check table name indeed exist.
            if (!RSQLite::dbExistsTable(conn = rsqliteconnection,
                                        name = tablename)) {
              assertcollection$push("Table does not exist.")
            }
            # Free connection.
            DBI::dbDisconnect(rsqliteconnection)
            
            # Get table.
            private$connection <-
              RSQLite::dbConnect(drv = RSQLite::SQLite(), dbname = host)
            private$table_df <-
              RSQLite::dbReadTable(conn = private$connection, name = private$tablename)
            
          },
          error = function(e) {
            assertcollection$push(
              paste(
                "Could not connect to sqlite or get table. host = ",
                private$host,
                ", tablename = ",
                private$tablename
                
              )
            )
          }, finally = {
            try(suppressWarnings(RSQLite::dbDisconnect(rsqliteconnection)), silent = T)
            # Halt on all the potential errors.
            checkmate::reportAssertions(collection = assertcollection)
            
          })
          
        }
        
        
        # Check that the table is well formed.
        private$table_types <-
          unlist(lapply(private$table_df, class))
        check_distinct_class_per_column <-
          length(private$table_types) == length(private$table_df)
        if (check_distinct_class_per_column == F) {
          assertcollection$push("Some columns have more than 1 data type listed.")
        }
        
        # Calculate hash for data frame
        private$table_hash <-
          private$hash_data_frame(df_to_hash = private$table_df)
        
        
      }# initialize function ends here
      ,
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Finalize ~
      #~~~~~~~~~~~~~~~~~~~~~~~~
      finalize = function() {
        print("Finalize object run.")
        if (private$dbtype == "sqlite") {
          # Clean up the connection.
          try(suppressWarnings(RSQLite::dbDisconnect(private$connection)), silent = T)
        }
      }# Finalize function ends here
    )# Public fields ends here
    ,
    #~~~~~~~~~~~~~~~~~~~~~~~~
    # ~ Private fields ~
    #~~~~~~~~~~~~~~~~~~~~~~~~
    
    private = list(
      dbtype = "",
      host = "",
      db = "",
      user = "",
      pass = "",
      tablename = "",
      connection = NA,
      table_df = NA,
      table_types = NA,
      table_hash = NA,
      
      # Func : hash data frame to compare 2 data frames for similarity.
      hash_data_frame = function(df_to_hash = NA) {
        # Define local sort function that handles classes not implementing sort.
        sorts <- function(x) {
          if (class(x) == "blob")
            x = as.integer(unlist(df_to_hash[, 4]))
          return(sort(x))
        }
        # Sort each column independently.
        df1 <- lapply(X = df_to_hash, FUN = sorts)
        # Digest MD5 on each column.
        df2 <-
          lapply(
            X = df1,
            FUN = function(x)
              digest::digest(object = x, algo = "md5")
          )
        # Digest the sorted md5 hashes to 1 final md5 value.
        df3 <-
          digest::digest(object = sort(unlist(df2)), algo = "md5")
        # Return 1 value.
        return(df3)
      }#func:hash_data_frame ends here
      
    )# Private fields ends here
  )# Class ends here
