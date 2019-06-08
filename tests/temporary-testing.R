

tmp <- list(a=1)
class(tmp) <- "myclass"
'[.myclass' <- function(o, x, y) {
  print(names(sys.call()))
  print(nargs() - 1)
  print(missing(x))
  print(missing(y))
}
tmp[,]  # Check nargs and missing!!!!!!! Solution found.
tmp[1]  
tmp[1, ]  



source("./src/objects.R")
rm(`[.SmallTableObject`)
'[.SmallTableObject' <- function(o, x, y) {
  print("overloaded:")
  print(nargs() - 1)
  print(missing(x))
  print(missing(y))
  print("end overloaded:")
  return()  
  print("call")

  tmp <- o$subset_read(x, y)
  print("end call")
  return(tmp)
}
sto2 <- NULL
sto2 <- SmallTableObject$new(dbtype = "sqlite", host = db, tablename = "test1")
sto2[2, ]
sto2[1]
sto2[, 1]
sto2[NULL]



