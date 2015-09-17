checkResponseIdentical <- function(response, input) {
  checkIdentical(unmeta(response), input)
}

checkResponseEquals <- function(response, input, tolerance=1) {
  response[,"price_c"] <- NULL
  checkEquals(unmeta(response), input, tolerance=tolerance)
}

test_SolrList_accessors <- function() {
  solr <- TestSolr()
  s <- SolrList(solr$uri)

  checkEquals(SolrCore(solr$uri), core(SolrList(solr$uri)))
  checkIdentical(SolrQuery(), query(s))
  
  doc <- list(id="1112211111", name="my name!")
  s[] <- NULL
  s[[doc$id]] <- doc
  checkResponseIdentical(s[[doc$id]], doc)
  checkResponseIdentical(eval(call("$", s, as.name(doc$id))), doc)
  checkIdentical(length(s), 1L)
  checkIdentical(names(s), doc$id)
  checkIdentical(ids(s), doc$id)
  checkIdentical(fieldNames(s), c("id", "name", "text"))

  docs <- list(
    list(id="2", inStock=TRUE, price=2, timestamp_dt=Sys.time()),
    list(id="3", inStock=FALSE, price=3, timestamp_dt=Sys.time()),
    list(id="4", price=4, timestamp_dt=Sys.time()),
    list(id="5", inStock=FALSE, price=5, timestamp_dt=Sys.time())
    )
  s[insert=TRUE] <- docs

  docs <- as(docs, "DocCollection")
  docs <- docs[,fieldNames(s)]
  
  checkIdentical(ids(s), c(docs[,"id"], doc$id))
  
  docs[,"timestamp_dt"] <- structure(docs[,"timestamp_dt"], tzone="UTC")
  ids(docs) <- docs[,"id"]
  
  checkResponseEquals(as.list(s[as.character(2:4)]), docs[1:3])
  checkIdentical(s[,"inStock"], c(docs[,"inStock"], NA))
  checkIdentical(s[rev(ids(docs)),"inStock"], rev(docs[,"inStock"]))

  s[[doc$id]] <- NULL
  checkResponseEquals(as.list(s), docs)
  s[as.character(2:4)] <- NULL
  checkResponseEquals(as.list(s), docs[4])
  s[] <- NULL
  checkResponseEquals(as.list(s), new("DocList"))

  docs[,"id"] <- NULL
  s[insert=TRUE] <- docs
  docs[,"id"] <- ids(docs)
  docs <- docs[,c("id", setdiff(fieldNames(docs), "id"))]
  checkResponseEquals(as.list(s), docs)
  s[] <- NULL

  ids <- ids(docs)
  ids(docs) <- NULL
  docs[,"id"] <- NULL
  s[ids,] <- docs
  ids(docs) <- ids
  docs[,"id"] <- ids(docs)
  docs <- docs[,c("id", setdiff(fieldNames(docs), "id"))]
  checkResponseEquals(as.list(s), docs)

  s[,"price"] <- s[,"price"] + 1L
  checkIdentical(s[,"price"], docs[,"price"] + 1L)
  s[,"price"] <- docs[,"price",drop=FALSE]
  checkIdentical(s[,"price"], docs[,"price"])
  s[,"price"] <- NULL
  noPrice <- docs
  noPrice[,"price"] <- NULL
  checkResponseEquals(as.list(s), noPrice)
  checkIdentical(s[,"price"], rep(NA_real_, 4))
  s[,"price"] <- docs[,"price"]
  
  i <- c("4", "5")
  s[i,"price"] <- docs[i,"price"] + 1L
  checkIdentical(s[i,"price"], docs[i,"price"] + 1L)
  s[i,"price"] <- NULL
  checkIdentical(s[i,"price"], c(NA_real_, NA_real_))

  s[] <- docs
  del <- list()
  del[as.character(2:4)] <- list(NULL)
  del <- c(del, list(doc))
  s[insert=TRUE] <- del
  dc <- as(list(doc), "DocCollection")
  ids(dc) <- doc$id
  checkResponseEquals(as.list(s[]), c(dc, docs["5"]))

  checkIdentical(as.list(s["foo"]), new("DocList", list()))
  checkIdentical(s[["foo"]], NULL)

  d5 <- docs[["5"]]
  df <- data.frame(id=c(doc$id, d5$id),
                   name=c(doc$name, NA),
                   price=c(NA, d5$price),
                   inStock=c(NA, FALSE),
                   timestamp_dt=as.POSIXct(c(NA, d5$timestamp_dt), "UTC",
                     "1970-01-01"),
                   stringsAsFactors=FALSE)
  checkResponseEquals(as.data.frame(s), as(df, "DocDataFrame"), tolerance=1)

  l <- as.list(s)
  dl <- c(dc, docs["5"])
  checkResponseEquals(l, dl)
}

test_SolrList_queries <- function() {
  solr <- rsolr:::TestSolr()
  s <- SolrList(solr$uri)

  docs <- list(
    list(id="2", inStock=TRUE, price=2, timestamp_dt=Sys.time()),
    list(id="3", inStock=TRUE, price=4, timestamp_dt=Sys.time()),
    list(id="4", inStock=TRUE, price=2, timestamp_dt=Sys.time()),
    list(id="5", inStock=FALSE, price=5, timestamp_dt=Sys.time())
    )
  s[] <- docs

  docs <- as(docs, "DocCollection")
  docs[,"timestamp_dt"] <- structure(docs[,"timestamp_dt"], tzone="UTC")
  ids(docs) <- as.character(rsolr:::pluck(docs, "id"))
  docs <- docs[,fieldNames(s)]

  ## CHECK: subset() queries
  ss <- subset(s, id %in% 2:4)
  checkResponseEquals(as.list(ss), docs[1:3])
  ss <- subset(s, id %in% 2:4, select=id:inStock)
  checkResponseEquals(as.list(ss), docs[1:3,c("id","price","inStock")])

  ## CHECK: sort
  sorted <- sort(s, by = ~ price)
  checkResponseEquals(as.list(sorted), docs[c(1, 3, 2, 4)])

  sorted <- sort(s, by = ~ price, decreasing=TRUE)
  checkResponseEquals(as.list(sorted), docs[c(4, 2, 1, 3)])

  sorted <- sort(s, by = ~ I(price * -1))
  checkResponseEquals(as.list(sorted), docs[c(4, 2, 1, 3)])
  
  ## CHECK: transform
  tformed <- transform(s, negPrice = price * -1)
  tform.docs <- docs
  tform.docs[,"negPrice"] <- tform.docs[,"price"] * -1
  checkResponseEquals(as.list(tformed), tform.docs)

  ## CHECK: unique()
### FIXME: Solr fails to facet on price_c, but does not throw error
  uniqueFields <- c("inStock", "price", "timestamp_dt", "includes")
  uniqueDocs <- docs[c(4,1,2),uniqueFields]
  uniqueDocs[,"includes"] <- NA_character_
  ids(uniqueDocs) <- NULL
  checkResponseEquals(unique(s[,uniqueFields,drop=FALSE]), uniqueDocs)

  ## CHECK: facet
  checkFacet <- function(formula, counts, solr=s) {
    expr.lang <- attr(terms(formula), "variables")[[2]]
    if (is.call(expr.lang) && expr.lang[[1]] == quote(cut)) {
      expr.name <- as.character(expr.lang[[2]])
    } else {
      expr.name <- deparse(eval(call("bquote", expr.lang)))
    }
    fct <- xtabs(formula, solr)
    correct.fct <- as.table(counts)
    dimnames(correct.fct) <- setNames(list(names(counts)), expr.name)
    checkIdentical(fct, correct.fct)
  }

  checkFacet(~ inStock, c("FALSE"=1L, "TRUE"=3L))  

  ## CHECK: facet on restricted query
  sub <- subset(s, inStock)
  checkFacet(~ inStock, c("FALSE"=0L, "TRUE"=3L), sub)

  df <- S3Part(as.data.frame(docs), TRUE)

  ## functional usage
  sa <- aggregate(price ~ inStock, s, min)
  correct.sa <- aggregate(price ~ inStock, df, min)
  colnames(correct.sa)[2L] <- "price.min"
  checkIdentical(sa, correct.sa)

  ## functional usage with argument
  sa <- aggregate(price ~ inStock, s, quantile, 0.25)
  correct.sa <- aggregate(price ~ inStock, df, quantile, 0.25)
  colnames(correct.sa)[2L] <- "price.percentile"
  correct.sa$price.percentile <- cbind("25%"=correct.sa$price.percentile)
  checkIdentical(sa, correct.sa)

  sa <- aggregate(~ inStock, s, ndoc)
  correct.sa <- as.data.frame(xtabs(~ inStock, df), responseName="ndoc")
  correct.sa$inStock <- as.logical(correct.sa$inStock)
  checkIdentical(sa, correct.sa)

  sa <- aggregate(~ inStock, s, function(df) {
                      data.frame(num=lengths(df$price), sum=sum(df$price))
                  })
  userFun <- function(df) {
      data.frame(num=length(df$price), sum=sum(df$price))
  }
  correct.sa <- data.frame(inStock=c(FALSE, TRUE),
                           do.call(rbind, by(df, df$inStock, userFun)),
                           row.names=NULL)
  checkIdentical(sa, correct.sa)

  sa <- aggregate(~ inStock, s, mean=sum(price)/lengths(price))
  correct.sa <- aggregate(price ~ inStock, df, mean)
  colnames(correct.sa)[2L] <- "mean"
  checkIdentical(sa, correct.sa)

  sa <- aggregate(price ~ inStock, s, heads, 1L)
  correct.sa <- aggregate(price ~ inStock, df, head, 1L)
  checkIdentical(sa, correct.sa)

  sa <- aggregate(price ~ inStock, s, heads, 1L, simplify=FALSE)
  correct.sa <- aggregate(price ~ inStock, df, head, 1L, simplify=FALSE)
  names(correct.sa$price) <- NULL
  checkIdentical(sa, correct.sa)

  sa <- aggregate(price ~ inStock, s, unique)
  correct.sa <- aggregate(price ~ inStock, df, unique)
  names(correct.sa$price) <- NULL
  checkIdentical(sa, correct.sa)
}
