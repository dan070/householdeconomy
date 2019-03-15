# tag with correct 

# Use the already tagged and corrected data in sqlite.
# Make 2-3-4-grams and train a naive bayes anew. Use beginning and ending of string as characters as well.
# And use complete string as token as well.
# Func to take a string and label, add begin/ending-markers ^$, split into N-grams, output 1 row/gram + label.
# Func to take bayes model, and a new string, and output highest prob label + probability.
# 
#
# For all data in "unlabeled", show that in handsontable.
# Let user go through and correct any wrongs by setting the manual ones.
# Then move data into "labeled" table.
#

library(rhandsontable)

DF = data.frame(val = 1:10, bool = TRUE, big = LETTERS[1:10],
                small = letters[1:10],
                dt = seq(from = Sys.Date(), by = "days", length.out = 10),
                stringsAsFactors = FALSE)

# try updating big to a value not in the dropdown
rhandsontable(DF, rowHeaders = NULL, width = 550, height = 300) %>%
  hot_col(col = "big", type = "dropdown", source = LETTERS) %>%
  hot_col(col = "small", type = "autocomplete", source = letters,
          strict = FALSE)


rhandsontable::editAddin()

DF

