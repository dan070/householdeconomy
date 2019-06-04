



# Objects used to simplify handling of data to and fro the database.


SmallTableObject <-
  R6::R6Class(
    classname = "SmallTableObject",
    
    #~~~~~~~~~~~~~~~~~~~~~~~~
    # ~ Public ~
    #~~~~~~~~~~~~~~~~~~~~~~~~
    public = list(
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Function: Subset ~
      # A rewrite of R:s special function [ and [<-
      # Used in tandem with S3-overloaded operator.
      # Handles both assignment and selection/subsetting operations.
      #~~~~~~~~~~~~~~~~~~~~~~~~
      subset = function(x,
                        y,
                        value = NULL) {

        # if paraemters are missing
        if(is.null(value) && missing(x) && missing(y)){
          print("1st if")
          return(private$table_df[,])
        }
        if(is.null(value) && !missing(x) && missing(y)){
          print("2nd if")
          return(private$table_df[x])
        }
        if(is.null(value) && missing(x) && !missing(y)){
          print("3d if")
          return(private$table_df[, y])
        }
        if(is.null(value) && !missing(x) && !missing(y)){
          print("4th if")
          if(is.null(x) && !is.null(y)) return(private$table_df[, y]) # Use y if specified but x null.
          if(is.null(y) && !is.null(x)) return(private$table_df[x, ]) # Use x if y is null.
          if(is.null(x) && is.null(y)) return(private$table_df[, ])
          return(private$table_df[x, y]) # Else use both x y, as specified.
        }

        
        
        # If parameter "value" is not null, requires updating the underlying data.
        if (!is.null(value)) {
          # Make copy and assign to that firstly to flush out errors implicitly using Rs own error checking.
          tmp_table_df <-
            private$table_df # Make a copy of the table df.
          
          # Row and column not specificed.
          if (is.null(x) && is.null(y)) {
            print("5th if statement.")
            tmp_table_df[x, y] <-
              value # Assign. x and y null so [,] <- whole.data.frame
          }
          
          # Row subset, column subset.
          if (!is.null(x) && !is.null(y)) {
            print("6th if statement.")
            tmp_table_df[x, y] <- value # Assign.
          }
          
          # Row empty, column subset.
          if (is.null(x) && !is.null(y)) {
            print("7th if statement.")
            tmp_table_df[, y] <- value # Assign.
          }
          
          # Row subset, column empty.
          if (!is.null(x) && is.null(y)) {
            print("8th if statement.")
            tmp_table_df[x, ] <- value # Assign.
          }
          
          # Assert: classes on temp data frame have not changed.
          tmp_class_value = lapply(X = tmp_table_df,
                                   FUN = class)
          tmp_class_df = lapply(X = private$table_df[, y],
                                FUN = class)
          checkmate::assert_set_equal(x = tmp_class_df,
                                      y = tmp_class_value,
                                      ordered = T)
          
          
          # Update the private data table.
          private$table_df[x, y] <- tmp_table_df
          
          # Save to data base.
          private$save_table_to_database()
          
          # Return
          return(self)
        }
      },
      
      
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Print ~
      #~~~~~~~~~~~~~~~~~~~~~~~~
      print = function(...) {
        cat("SmallTableObject\n")
        cat(paste("db type : ", private$dbtype, "\n"))
        cat(paste("table name : ", private$tablename, "\n"))
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
                                choices = private$allowed_dbtypes,
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
        
        # Check: data types are simple.
        private$table_types <-
          unlist(lapply(private$table_df, class))
        check_distinct_class_per_column <-
          length(private$table_types) == length(private$table_df)
        if (check_distinct_class_per_column == F) {
          assertcollection$push("Some columns have more than 1 data type listed.")
        }
        
        # Save md5-hash for data frame.
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
      ## TODO: remove "connection" field, and open connection when needed. Else errors like
      # Warning message:
      # call dbDisconnect() when finished working with a connection 
      table_df = NA,
      table_types = NA,
      table_hash = NA,
      allowed_dbtypes = c("sqlite"),
      
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Private func: hash_data_frame
      #~~~~~~~~~~~~~~~~~~~~~~~~
      
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
      ,
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Private func: save_table
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # Func : save the local data frame to our data base.
      save_table_to_database = function() {
        if (private$dbtype == "sqlite") {
          # ~~ Check data base connection working ~~
          if (!DBI::dbIsValid(private$connection)) {
            private$connection <-
              RSQLite::dbConnect(drv = RSQLite::SQLite(), dbname = private$host)
          }
          checkmate::assert_true(DBI::dbIsValid(private$connection))
          
          # ~~ Check underlying table integrity ~~
          # (ensure no data race on local table versus target table).
          # ie... Download table. Hash it. Compare hashes. Else error.
          tmp_data_table <-
            DBI::dbReadTable(conn = private$connection, name = private$tablename)
          tmp_hash <-
            private$hash_data_frame(df_to_hash = tmp_data_table)
          checkmate::assert_true(private$table_hash, tmp_hash)
          
          # ~~ Upsert the table ~~
          # Truncate the data base table.
          DBI::dbGetQuery(
            conn = private$connection,
            statement = paste("DELETE FROM ", private$tablename)
          )
          # Upsert whole private table to data base table.
          DBI::dbWriteTable(
            conn = private$connection,
            name = private$tablename,
            value = private$table_df
          )
          # Update private hash (of new table).
          private$table_hash <-
            private$hash_data_frame(private$table_df)
        } # if private$dbtype == "sqlite" ends here.
      }# func:save_table_to_database ends here.
    )# Private fields ends here
  )# Class ends here




#~~~~~~~~~~~~~~~~~~~~~~~~
# Overloading
# [ and [<- operators.
#~~~~~~~~~~~~~~~~~~~~~~~~

rm(`[.SmallTableObject`)
'[.SmallTableObject' <- function(o = NULL,
                                 x = NULL,
                                 y = NULL) {
  tmp <- o$subset(x = x, y = y, value = NULL)
  return(tmp)
}



rm(`[<-.SmallTableObject`)
'[<-.SmallTableObject' <-
  function(o = NULL,
           x = NULL,
           y = NULL,
           value = NULL) {
    tmp <- o$subset(x = x, y = y, value = value)
    return(tmp)
  }
