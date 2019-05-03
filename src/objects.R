



# Objects used to simplify handling of data to and fro the database.


SmallTableObject <-
  R6::R6Class(
    classname = "SmallTableObject",
    
    #~~~~~~~~~~~~~~~~~~~~~~~~
    # ~ Public ~
    #~~~~~~~~~~~~~~~~~~~~~~~~
    public = list(
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Get ~
      #~~~~~~~~~~~~~~~~~~~~~~~~
      get = function() {
        return(private$table_df)
      },
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Set ~
      #~~~~~~~~~~~~~~~~~~~~~~~~
      set = function(df) {
        
        # compare the form of input df to the object private df.
        assertcollection <- checkmate::makeAssertCollection()
        # Check for number of cols and types.
        checkmate::assertDataFrame(
          x = df,
          types = private$table_types,
          ncols = length(private$table_df),
          add = assertcollection
        )
        # Check names of columns.
        checkmate::assertSetEqual(names(df),
                                  names(private$table_df),
                                  ordered = T,
                                  add = assertcollection)
        
        # Check table integrity (ensure no data race on local table versus target table).
        tryCatch(
          expr = {
            # Get target table (again)
            table_verification <-
              RSQLite::dbReadTable(conn = private$connection, name = private$tablename)
            # Check classes 
            checkmate::assertDataFrame(x = private$table_df,
                                       types = unlist(lapply(table_verification, class)),
                                       add = assertcollection)
            # Check names
            checkmate::assertSetEqual(
              names(private$table_df),
              names(table_verification),
              ordered = T,
              add = assertcollection
            )
            
          },
          error = function(e) {
            assertcollection$push("Could not read the table on data base side, while checking for data integrity.")
          },
          finally = {
          }
        )
        
          # Do hashing of the columns, and compare.
          hash_1 <- private$hash_data_frame(table_verification)
          hash_2 <- private$table_hash
          if(hash_1 != hash_2)
            assertcollection$push("")

      },

      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Print ~
      #~~~~~~~~~~~~~~~~~~~~~~~~
      print = function(...) {
        print("SmallTableObject")
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
        private$table_hash <- private$hash_data_frame(df_to_hash = private$table_df)
        
        
      }# initialize function ends here
      ,
      #~~~~~~~~~~~~~~~~~~~~~~~~
      # ~ Finalize ~
      #~~~~~~~~~~~~~~~~~~~~~~~~
      finalize = function() {
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
      hash_data_frame = function(df_to_hash = NA){
        # Define local sort function that handles classes not implementing sort.
        sorts <- function(x){
          if(class(x) == "blob") x = as.integer(unlist(df_to_hash[, 4]))
          return(sort(x))
        }
        # Sort each column independently.
        df1 <- lapply(X = df_to_hash, FUN = sorts)
        # Digest MD5 on each column.
        df2 <- lapply(X = df1, FUN = function(x) digest::digest(object = x, algo = "md5"))
        # Digest the sorted md5 hashes to 1 final md5 value.
        df3 <- digest::digest(object = sort(unlist(df2)), algo = "md5")
        # Return 1 value.
        return(df3)
      }#func:hash_data_frame ends here
      
    )# Private fields ends here
  )# Class ends here




