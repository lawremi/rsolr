checkResponseIdentical <- function(response, input) {
  checkIdentical(unmeta(response), input)
}

checkResponseEquals <- function(response, input, tolerance=1) {
  response[,"price_c"] <- NULL
  checkEquals(unmeta(response), input, tolerance=tolerance)
}

test_Solr_accessors <- function() {
  solr <- rsolr:::TestSolr()
  s <- Solr(solr$uri)

  checkEquals(SolrCore(solr$uri), core(s))
  checkIdentical(SolrQuery(), query(s))
  
  doc <- list(id="1112211111", name="my name!")
  s[[doc$id]] <- doc
  checkResponseIdentical(s[[doc$id]], doc)
  checkResponseIdentical(eval(call("$", s, as.name(doc$id))), doc)
  checkIdentical(nrow(s), 1L)
  checkIdentical(names(s), doc$id)

  docs <- list(
    list(id="2", inStock=TRUE, price=2, timestamp_dt=Sys.time()),
    list(id="3", inStock=FALSE, price=3, timestamp_dt=Sys.time()),
    list(id="4", price=4, timestamp_dt=Sys.time()),
    list(id="5", inStock=FALSE, price=5, timestamp_dt=Sys.time())
    )
  s[insert=TRUE] <- docs

  docs <- as(docs, "DocCollection")
  docs[,"timestamp_dt"] <- structure(docs[,"timestamp_dt"], tzone="UTC")
  ids(docs) <- docs[,"id"]
  
  checkResponseEquals(as.list(s[as.character(2:4)]), docs[1:3])
  checkIdentical(s[,"inStock"], c(docs[,"inStock"], NA))
  checkIdentical(s[rev(ids(docs)),"inStock"], rev(docs[,"inStock"]))

  s[[doc$id]] <- NULL
  checkResponseEquals(as.list(s[]), docs)
  s[as.character(2:4)] <- NULL
  checkResponseEquals(as.list(s[]), docs[4])
  s[] <- NULL
  checkResponseEquals(as.list(s[]), new("DocList"))

  docs[,"id"] <- NULL
  s[insert=TRUE] <- docs
  docs[,"id"] <- ids(docs)
  checkResponseEquals(as.list(s[]), docs)
  s[] <- NULL

  ids <- ids(docs)
  ids(docs) <- NULL
  docs[,"id"] <- NULL
  s[ids,] <- docs
  ids(docs) <- ids
  docs[,"id"] <- ids(docs)
  checkResponseEquals(as.list(s[]), docs)

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
                   timestamp_dt=as.POSIXct(c(NA, d5$timestamp_dt), "UTC",
                     "1970-01-01"),
                   price=c(NA, d5$price),
                   name=c(doc$name, NA),
                   inStock=c(NA, FALSE),
                   stringsAsFactors=FALSE)
  rownames(df) <- df$id
  checkResponseEquals(as.data.frame(s), as(df, "DocDataFrame"), tolerance=1)

  l <- as.list(s)
  dl <- c(dc, docs["5"])
  checkResponseEquals(l, dl)
  
  solr$kill()
}

test_Solr_queries <- function() {
  solr <- rsolr:::TestSolr()
  s <- Solr(solr$uri)

  docs <- list(
    list(id="2", inStock=TRUE, price=2, timestamp_dt=Sys.time()),
    list(id="3", inStock=TRUE, price=4, timestamp_dt=Sys.time()),
    list(id="4", inStock=TRUE, price=3, timestamp_dt=Sys.time()),
    list(id="5", inStock=FALSE, price=5, timestamp_dt=Sys.time())
    )
  s[] <- docs

  docs <- as(docs, "DocCollection")
  docs[,"timestamp_dt"] <- structure(docs[,"timestamp_dt"], tzone="UTC")
  ids(docs) <- as.character(rsolr:::pluck(docs, "id"))

  ## CHECK: subset() queries
  ss <- subset(s, id %in% 2:4)
  checkResponseEquals(as.list(ss), docs[1:3])

  ## CHECK: sort
  sorted <- sort(s, by = ~ price)
  checkResponseEquals(as.list(sorted), docs[c(1, 3, 2, 4)])

  sorted <- sort(s, by = ~ price, decreasing=TRUE)
  checkResponseEquals(as.list(sorted), docs[c(4, 2, 3, 1)])

  sorted <- sort(s, by = ~ I(price * -1))
  checkResponseEquals(as.list(sorted), docs[c(4, 2, 3, 1)])
  
  ## CHECK: transform
  tformed <- transform(s, negPrice = price * -1)
  tform.docs <- docs
  tform.docs[,"negPrice"] <- tform.docs[,"price"] * -1
  checkResponseEquals(as.list(tformed), tform.docs)
  
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

  ## CHECK: df upload and statistics
  val_pi <- as.integer(c(23, 26, 38, 46, 55, 63, 77, 84, 92, 94))
  price <- val_pi^2
  df <- data.frame(id=as.character(seq_along(val_pi)),
                   name=paste0("doc:", val_pi), val_pi,
                   price, inStock=rep(c(TRUE, FALSE), length=length(val_pi)),
                   stringsAsFactors=FALSE)
  s[] <- df
  sm <- summary(s, of="val_pi")
  stats.df <- as.data.frame(stats(sm))
  statsDf <- function(x, field=deparse(substitute(x))) {
    data.frame(field, min=as.numeric(min(x)), max=as.numeric(max(x)),
               sum=as.numeric(sum(x)), count=as.numeric(length(x)),
               missing=as.numeric(sum(is.na(x))),
               sumOfSquares=sum(x^2), mean=mean(x),
               stddev=sd(x), row.names=field, stringsAsFactors=FALSE)
  }
  correct.stats.df <- statsDf(val_pi)  
  checkIdentical(stats.df, correct.stats.df)

  df$price[3] <- NA
  s[] <- df

  checkAggIdentical <- function(test, correct) {
    correct[[1L]] <- as.factor(correct[[1L]])
    checkIdentical(test, correct)
  }
  
  sa <- aggregate(price ~ inStock, s, min)
  correct.sa <- aggregate(price ~ inStock, df, min, na.action=na.pass)
  checkAggIdentical(sa, correct.sa)

  sa <- aggregate(price ~ inStock, s, min, na.rm=TRUE)
  correct.sa <- aggregate(price ~ inStock, df, min)
  checkAggIdentical(sa, correct.sa)

  sa <- aggregate(price ~ inStock, s, count)
  correct.sa <- aggregate(price ~ inStock, df, length, na.action=na.pass)
  correct.sa$price <- as.numeric(correct.sa$price)
  checkAggIdentical(sa, correct.sa)
  
  sa <- aggregate(price ~ inStock, s, count, na.rm=TRUE)
  correct.sa <- aggregate(price ~ inStock, df, length)
  correct.sa$price <- as.numeric(correct.sa$price)
  checkAggIdentical(sa, correct.sa)

  sa <- aggregate(~ inStock, s, nrow)
  correct.sa <- as.data.frame(xtabs(~ inStock, df))
  correct.sa$Freq <- as.integer(correct.sa$Freq)
  checkIdentical(sa, correct.sa)

  sa <- aggregate(price ~ inStock, s, var, na.rm=TRUE)
  correct.sa <- aggregate(price ~ inStock, df, var)
  correct.sa[[1L]] <- as.factor(correct.sa[[1L]])
  checkEquals(sa, correct.sa)

  sa <- aggregate(~ inStock, s, head, 2L)
  sa$price_c <- NULL
  correct.sa <- do.call(rbind, by(df, df$inStock, head, 2L))
  correct.sa <- correct.sa[unique(c("inStock", colnames(correct.sa)))]
  correct.sa$inStock <- as.factor(correct.sa$inStock)
  rownames(correct.sa) <- NULL
  checkAggIdentical(sa, correct.sa)

  sa <- aggregate(price ~ inStock, s, head, 1L)
  correct.sa <- aggregate(price ~ inStock, df, head, 1L)
  checkAggIdentical(sa, correct.sa)

  sa <- aggregate(price ~ inStock, s, unique)
  correct.sa <- subset(as.data.frame(xtabs(~ inStock + price, df)), Freq > 0L,
                       select=-Freq)
  correct.sa <- correct.sa[order(correct.sa[[1L]]),]
  checkAggIdentical(sa, correct.sa)
  
  solr$kill()
}
