f1 <- function(x, y, value){
  
  print(names(sys.call()))
  print(missing(x))
  print(missing(y))
  print(missing(value))
  
}

f1()
f1(x = 1)
f1(, y = 1)
f1(NULL)
f1(NULL, y = 2)

`[.data.frame`


source("./src/objects.R")
rm(`[.SmallTableObject`)
'[.SmallTableObject' <- function(o,
                                 x,
                                 y) {
  print("overloaded:")
  #print(o)
  #print(x)
  #print(y)
  print("end overloaded:")
  
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



