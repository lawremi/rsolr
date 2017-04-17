## Some inspiration for these tests:
## https://svn.apache.org/repos/asf/lucene/dev/trunk/solr/solrj/src/test/org/apache/solr/client/solrj/SolrExampleTests.java

checkResponseIdentical <- function(response, input) {
  checkIdentical(unmeta(response), input)
}

checkResponseEquals <- function(response, input, tolerance=1) {
  response[,"price_c"] <- NULL
  checkEquals(unmeta(response), input, tolerance=tolerance)
}

test_SolrQuery <- function() {
  solr <- TestSolr()
  s <- SolrList(solr$uri)
  sc <- core(s)

  docs <- list(
    list(id="2", inStock=TRUE, price=2, weight=1, timestamp_dt=Sys.time()),
    list(id="3", inStock=TRUE, price=4, weight=5, timestamp_dt=Sys.time()),
    list(id="4", price=3, weight=3, timestamp_dt=Sys.time()),
    list(id="5", inStock=FALSE, price=5, timestamp_dt=Sys.time())
    )
  s[] <- docs

  docs <- as(docs, "DocCollection")
  docs[,"timestamp_dt"] <- structure(docs[,"timestamp_dt"], tzone="UTC")
  ids(docs) <- as.character(rsolr:::pluck(docs, "id"))
  docs <- docs[,fieldNames(s)]

  .testSubset(s, docs)
  .testBoundsRestriction(sc, docs)
  .testSort(sc, docs)
  .testTransform(sc, docs)
  .testFacets(s, docs)
}

.testSubset <- function(s, docs) {
    query <- SolrQuery()
    sc <- core(s)
    subset.query <- subset(query, id %in% 3:5)
    checkResponseEquals(read(sc, subset.query), docs[2:4])
    subset.query <- subset(subset.query, fields=c("id", "inStock"))
    checkResponseEquals(read(sc, subset.query),
                        docs[2:4, c("id", "inStock")])
    subset.query <- subset(subset.query, fields="*")
    checkResponseEquals(read(sc, subset.query), docs[2:4])
    subset.query <- subset(subset.query, inStock)
    checkResponseEquals(read(sc, subset.query), docs[2])

    query <- SolrQuery()
    subset.query <- subset(query, price > 2)
    checkResponseEquals(read(sc, subset.query), docs[2:4])
    subset.query <- subset(query, price < 5)
    checkResponseEquals(read(sc, subset.query), docs[1:3])
    subset.query <- subset(query, price <= 4)
    checkResponseEquals(read(sc, subset.query), docs[1:3])
    subset.query <- subset(query, price >= 5)
    checkResponseEquals(read(sc, subset.query), docs[4])
    subset.query <- subset(query, price == 5)
    checkResponseEquals(read(sc, subset.query), docs[4])
    subset.query <- subset(query, price != 5)
    checkResponseEquals(read(sc, subset.query), docs[1:3])
    subset.query <- subset(query, !(price != 5))
    checkResponseEquals(read(sc, subset.query), docs[4])
    subset.query <- subset(query, price > 2 & price < 5)
    checkResponseEquals(read(sc, subset.query), docs[2:3])
    subset.query <- subset(query, price <= 2 | price >= 5)
    checkResponseEquals(read(sc, subset.query), docs[c(1,4)])
    subset.query <- subset(query, !(price <= 2 | price >= 5))
    checkResponseEquals(read(sc, subset.query), docs[2:3])
    subset.query <- subset(query, (price + 1) > 3)
    checkResponseEquals(read(sc, subset.query), docs[2:4])
    subset.query <- subset(query, is.na(inStock))
    checkResponseEquals(read(sc, subset.query), docs[3])
    subset.query <- subset(query, grepl("[0-4]", id))
    checkResponseEquals(read(sc, subset.query), docs[1:3])
    subset.query <- subset(query, grepl("[0-4]", id, fixed=TRUE))
    checkResponseEquals(read(sc, subset.query), unname(docs[NULL]))
    subset.query <- subset(query, grepl("2", id, fixed=TRUE))
    checkResponseEquals(read(sc, subset.query), docs[1])
    subset.query <- subset(query, (price + 1) > 3 & (price - 1) < 4)
    checkResponseEquals(read(sc, subset.query), docs[2:3])
    subset.query <- subset(query, price > 3 & FALSE)
    checkResponseEquals(read(sc, subset.query), unname(docs[NULL]))
    subset.query <- subset(query, TRUE & price > 3)
    checkResponseEquals(read(sc, subset.query), docs[c(2,4)])
    subset.query <- subset(query, TRUE | price > 3)
    checkResponseEquals(read(sc, subset.query), docs)
    subset.query <- subset(query, price > weight)
    checkResponseEquals(read(sc, subset.query), docs[1])
    subset.query <- subset(query, !(price > weight))
    checkResponseEquals(read(sc, subset.query), docs[2:3])
    subset.query <- subset(query, price <= weight)
    checkResponseEquals(read(sc, subset.query), docs[2:3])
    subset.query <- subset(query, price < weight)
    checkResponseEquals(read(sc, subset.query), docs[2])
    subset.query <- subset(query, price >= weight)
    checkResponseEquals(read(sc, subset.query), docs[c(1,3)])
    subset.query <- subset(query, price %/% weight == 0)
    checkResponseEquals(read(sc, subset.query), docs[2])
    subset.query <- subset(query, !(price %/% weight == 0))
    checkResponseEquals(read(sc, subset.query), docs[c(1,3)])
    subset.query <- subset(query, trunc(-(weight/price)) == -1)
    checkResponseEquals(read(sc, subset.query), docs[2:3])

    joindocs <- as(list(
        list(id="6", links="4"),
        list(id="7", links="2")
        ), "DocList")
    names(joindocs) <- joindocs[,"id"]
    s[ids(joindocs)] <- joindocs
    subset.query <- subset(query, links %in% id[price > 2])
    checkResponseEquals(read(sc, subset.query), joindocs[1])
    subset.query <- subset(query, links %in% id)
    checkResponseEquals(read(sc, subset.query), joindocs)

    s[] <- docs
}

.testBoundsRestriction <- function(sc, docs) {
    query <- SolrQuery()
    head.query <- head(query, 2L)
    checkResponseEquals(read(sc, head.query), head(docs, 2L))
    tail.query <- tail(query, 2L)
    checkResponseEquals(read(sc, tail.query), tail(docs, 2L))
    tail.query <- tail(query, -3L)
    checkResponseEquals(read(sc, tail.query), tail(docs, -3L))
    head.query <- head(query, -3L)
    checkResponseEquals(read(sc, head.query), head(docs, -3L))
    restricted.query <- tail(head(query, -2L), 1L)
    checkResponseEquals(read(sc, restricted.query), tail(head(docs, -2L), 1L))
    restricted.query <- head(tail(query, -2L), 1L)
    checkResponseEquals(read(sc, restricted.query), head(tail(docs, -2L), 1L))
    restricted.query <- head(tail(query, 2L), -1L)
    checkResponseEquals(read(sc, restricted.query), head(tail(docs, 2L), -1L))
    restricted.query <- tail(head(query, 2L), -1L)
    checkResponseEquals(read(sc, restricted.query), tail(head(docs, 2L), -1L))
    restricted.query <- head(tail(query, -1L), 2L)
    checkResponseEquals(read(sc, restricted.query), head(tail(docs, -1L), 2L))
    restricted.query <- tail(head(query, -2L), -1L)
    checkResponseEquals(read(sc, restricted.query), tail(head(docs, -2L), -1L))
    restricted.query <- tail(head(query, -1L), -2L)
    checkResponseEquals(read(sc, restricted.query), tail(head(docs, -1L), -2L))
    restricted.query <- head(tail(query, -2L), -1L)
    checkResponseEquals(read(sc, restricted.query), head(tail(docs, -2L), -1L))
    restricted.query <- head(tail(query, 2L), 1L)
    checkResponseEquals(read(sc, restricted.query), head(tail(docs, 2L), 1L))
    restricted.query <- tail(head(query, 2L), 1L)
    checkResponseEquals(read(sc, restricted.query), tail(head(docs, 2L), 1L))
}

.testSort <- function(sc, docs) {
    query <- SolrQuery()
    sorted.query <- sort(query, by = ~ price)
    checkResponseEquals(read(sc, sorted.query), docs[c(1, 3, 2, 4)])
    sorted.query <- sort(query, by = ~ price, decreasing=TRUE)
    checkResponseEquals(read(sc, sorted.query), docs[c(4, 2, 3, 1)])
    reverse.query <- rev(sorted.query)
    checkResponseEquals(read(sc, reverse.query), rev(docs[c(4, 2, 3, 1)]))
    sorted.query <- sort(query, by = ~ I(weight * -1))
    checkResponseEquals(read(sc, sorted.query), docs[c(2, 3, 1, 4)])
    sorted.query <- sort(query, by = ~ I(weight * -1), decreasing=TRUE)
    checkResponseEquals(read(sc, sorted.query), docs[c(1, 3, 2, 4)])
    sorted.query <- sort(query, by = ~ id, decreasing=TRUE)
    checkResponseEquals(read(sc, sorted.query), rev(docs))
}

.testTransform <- function(sc, docs) {
    query <- SolrQuery()

    tform.query <- transform(query, stocked=inStock)
    tform.docs <- docs
    tform.docs[,"stocked"] <- tform.docs[,"inStock"]
    tform.docs["4","stocked"] <- NULL
    checkResponseEquals(read(sc, tform.query), tform.docs)

    tform.query <- transform(query, membership = id %in% 3:5)
    tform.docs <- docs
    tform.docs[,"membership"] <- tform.docs[,"id"] %in% 3:5
    checkResponseEquals(read(sc, tform.query), tform.docs)

    tform.query <- transform(query, equals = weight == 3)
    tform.docs <- docs
    tform.docs[,"equals"] <- tform.docs[,"weight"] == 3
    tform.docs["5","equals"] <- NULL
    checkResponseEquals(read(sc, tform.query), tform.docs)
    
    tform.query <- transform(query, overpriced=price > weight)
    tform.docs <- docs
    tform.docs[,"overpriced"] <- tform.docs[,"price"] > tform.docs[,"weight"]
    tform.docs["5","overpriced"] <- NULL
    checkResponseEquals(read(sc, tform.query), tform.docs)
    
    tform.query <- transform(query, negPrice = price * -1)
    tform.docs <- docs
    tform.docs[,"negPrice"] <- tform.docs[,"price"] * -1
    checkResponseEquals(read(sc, tform.query), tform.docs)

    tform.query <- transform(query, naPrice = price * NA)
    checkResponseEquals(read(sc, tform.query), docs)

    tform.query <- transform(query, inStock2 = inStock & NA)
    checkIdentical(read(sc, tform.query)[,"inStock2"],
                   docs[,"inStock"] & NA)

    tform.query <- transform(query, inStock2 = inStock | NA)
    checkIdentical(read(sc, tform.query)[,"inStock2"],
                   docs[,"inStock"] | NA)

    tform.query <- transform(query, price = price * -1)
    tform.docs <- docs
    tform.docs[,"price"] <- tform.docs[,"price"] * -1
    checkResponseEquals(read(sc, tform.query), tform.docs)
    
    tform.query <- transform(query, price = round(price/3))
    tform.docs <- docs
    tform.docs[,"price"] <- round(tform.docs[,"price"]/3)
    checkResponseEquals(read(sc, tform.query), tform.docs)

    tform.query <- transform(query, pmin = pmin(price, weight),
                             pmax = pmax(price, weight))
    tform.docs <- docs
    tform.docs[,"pmin"] <- pmin(tform.docs[,"price"], tform.docs[,"weight"])
    tform.docs[,"pmax"] <- pmin(tform.docs[,"price"], tform.docs[,"weight"])
    tform.docs["5",c("pmax", "pmin")] <- NULL
    checkResponseEquals(read(sc, tform.query), tform.docs)

    tform.query <- transform(query, pmin = pmin(price, weight, na.rm=TRUE),
                             pmax = pmax(price, weight, na.rm=TRUE))
    tform.docs <- docs
    tform.docs[,"pmin"] <- pmin(tform.docs[,"price"], tform.docs[,"weight"],
                                na.rm=TRUE)
    tform.docs[,"pmax"] <- pmin(tform.docs[,"price"], tform.docs[,"weight"],
                                na.rm=TRUE)
    checkResponseEquals(read(sc, tform.query), tform.docs)

    tform.query <- transform(query, bins = cut(weight, seq(1, 5, 2)))
    tform.docs <- docs
    tform.docs[,"bins"] <- cut(tform.docs[,"weight"], seq(1, 5, 2))
    tform.docs[c("2", "5"),"bins"] <- NULL
    checkIdentical(as.integer(read(sc, tform.query)[,"bins"]),
                   as.integer(tform.docs[,"bins"]))
}

.testFacets <- function(s, docs) {
    sc <- core(s)
    checkTable <- function(formula, counts, query = SolrQuery())
        {
            vars <- as.list(attr(terms(formula), "variables"))[-1L]
            expr.name <- vapply(vars, function(v) {
                                    deparse(eval(call("bquote", v)))
                                }, character(1L))
            facet.query <- xtabs(formula, query)
            fct <- as.table(facets(sc, facet.query)[[expr.name]])
            correct.fct <- as.table(counts)
            dimnames(correct.fct) <- setNames(dimnames(correct.fct), expr.name)
            checkIdentical(fct, correct.fct)
        }

    checkTable(~ inStock, c("FALSE"=1L, "TRUE"=2L))
    checkTable(~ !inStock, c("FALSE"=2L, "TRUE"=1L))
### SOLRBUG: numeric faceting returns 0.0:0 buckets using our params
### https://issues.apache.org/jira/browse/SOLR-7496
###    checkTable(~ price, c("2.0"=1L, "3.0"=1L, "4.0"=1L, "5.0"=1L))
    checkTable(~ price > 3, c("FALSE"=2L, "TRUE"=2L))
    checkTable(~ price > 10, c("FALSE"=4L, "TRUE"=0L))
    checkTable(~ price > 3 & inStock, c("FALSE"=3L, "TRUE"=1L))
    checkTable(~ price > log2(8), c("FALSE"=2L, "TRUE"=2L))
    checkTable(~ cut(price, seq(1, 5, 2)), c("(1,3]"=2L, "(3,5]"=2L))
### FIXME: waiting on interval facets in the JSON API
    ## checkTable(~ cut(price, c(-Inf, 2, 4, 5, Inf)),
    ##            c("(-Inf,2]"=1L, "(2,4]"=2L, "(4,5]"=1L, "(5, Inf]"=0L))
    query <- SolrQuery()
    tform.query <- transform(query, bins = cut(price, seq(1, 5, 2)))
    checkTable(~ bins, c("(1,3]"=2L, "(3,5]"=2L), query=tform.query)
### FIXME: SOLR bug 7496
### facet.query <- xtabs(~ price, query)
    facet.query <- query
    facet.query <- xtabs(~ inStock, facet.query)
    facet.query <- xtabs(~ price > 2 + 1, facet.query)
    facet.query <- xtabs(~ price - 2 > 1, facet.query)
    fct <- facets(sc, facet.query)
    ## tab <- as.table(c("2.0"=1L, "3.0"=1L, "4.0"=1L, "5.0"=1L))
    ## names(dimnames(tab)) <- "price"
    ## checkIdentical(fct$price,  tab)
    tab <- as.table(c("FALSE"=1L, "TRUE"=2L))
    names(dimnames(tab)) <- "inStock"
    checkIdentical(as.table(fct$inStock),  tab)
    tab <- as.table(c("FALSE"=2L, "TRUE"=2L))
    names(dimnames(tab)) <- "price > 2 + 1"
    checkIdentical(as.table(fct$"price > 2 + 1"),  tab)
    names(dimnames(tab)) <- "price - 2 > 1"
    checkIdentical(as.table(fct$"price - 2 > 1"), tab)
    checkIdentical(length(facets(sc, query)), 0L)
    facet.query <- facet(query, ~ inStock, sort=~count, decreasing=TRUE)
    fct <- facets(sc, facet.query)
    tab <- as.table(c("TRUE"=2L, "FALSE"=1L))
    names(dimnames(tab)) <- "inStock"
    checkIdentical(as.table(fct$inStock),  tab)
    sub.query <- subset(query, inStock)
    checkTable(~ inStock, c("FALSE"=0L, "TRUE"=2L), sub.query)
    sub.query <- subset(facet.query, inStock)
    fct <- facets(sc, sub.query)
    checkIdentical(as.table(fct$inStock), tab)

    ## Adapted this dataset and testset from the Solr JSON Facet tests
    df <- data.frame(id=as.character(1:6),
                     cat_s=c("A", "B", NA, "A", "B", "B"),
                     where_s=c("NY", "NJ", NA, "NJ", "NJ", "NY"),
                     num_d=c(4, -9, NA, 2, NA, -5),
                     num_i=c(NA, -5, NA, 3, 7, -5),
                     super_s=c("zodiac", "superman", NA, "spiderman", "batman",
                         "hulk"),
                     date_dt=c("2001-01-01T01:01:01Z", "2002-02-02T02:02:02Z",
                         NA, "2003-03-03T03:03:03Z", "2001-02-03T01:02:03Z",
                         "2002-03-01T03:02:01Z"),
                     val_b=c(TRUE, FALSE, NA, NA, NA, NA),
                     sparse_s=c("one", NA, NA, NA, "two", NA),
                     multi_ss=I(list(NULL, c("a", "b"), NULL, "b", "a",
                         c("b", "a"))),
                     stringsAsFactors=FALSE)
    parseSolrDate <- function(x) { # because we copy/paste from Solr tests
        as.POSIXct(strptime(x, "%Y-%m-%dT%H:%M:%SZ", tz="UTC"))
    }
    df$date_dt <- parseSolrDate(df$date_dt)
    s[] <- df

    query <- SolrQuery()
    toTable <- function(tab) {
        class(tab) <- "table"
        attr(tab, "call") <- NULL
        tab
    }
    checkTable2 <- function(formula, subset, ...) {
        tab <- xtabs(formula, df, exclude=NULL, ...)
        fct <- facets(sc, xtabs(formula, query, exclude=NULL, ...))[[formula]]
        checkIdentical(as.table(fct), toTable(tab))
    }

### CHECK: missing
    checkTable2(~ val_b, na.action=na.pass)

### CHECK: nested facets
    checkTable2(~ cat_s + I(where_s == "NJ"))
    checkTable2(~ I(cat_s == "B") + I(where_s == "NJ"))

### CHECK: multiple nested facets
    facet.query <- query
    facet.query <- xtabs(~ I(cat_s == "B") + I(where_s == "NJ"), facet.query)
    facet.query <- xtabs(~ I(cat_s == "B") + I(where_s == "NY"), facet.query)
    fct <- facets(sc, facet.query)
    tab <- xtabs(~ I(cat_s == "B") + I(where_s == "NJ"), df)
    checkIdentical(as.table(fct[[1L]][[1L]]), toTable(tab))
    tab <- xtabs(~ I(cat_s == "B") + I(where_s == "NY"), df)
    checkIdentical(as.table(fct[[1L]][[2L]]), toTable(tab))

### CHECK: missing with stat
    facet.query <- facet(query, ~ cat_s, sum(num_d), useNA=TRUE)
    stats <- stats(facets(sc, facet.query)[[1L]])
    stats$count <- NULL
    dfna <- df
    dfna$cat_s <- factor(df$cat_s, exclude=NULL)
    agg <- aggregate(num_d ~ cat_s, dfna, sum, na.action=na.pass)
    agg$cat_s <- as.character(agg$cat_s)
    colnames(agg)[2L] <- "num_d.sum"
    checkIdentical(stats, agg)

### CHECK: nested facet with stat
    facet.query <- query
    facet.query <- facet(facet.query, ~ I(cat_s == "B") + I(where_s == "NJ"),
                         sum(num_d))
    stats <- stats(facets(sc, facet.query)[[1L]][[1L]])
    stats$count <- NULL
    agg <- aggregate(num_d ~ I(cat_s == "B") + I(where_s == "NJ"), df, sum,
                     na.action=na.pass)
    colnames(agg)[3L] <- "num_d.sum"
    agg[1:2] <- lapply(agg[1:2], unclass)
    checkIdentical(stats, agg)
    
### CHECK: sort on stat
    facet.query <- facet(query, ~ cat_s, n1=sum(num_d), sort=~n1)
    stats <- stats(facets(sc, facet.query)[[1L]])
    stats$count <- NULL
    agg <- aggregate(num_d ~ cat_s, df, sum, na.action=na.pass)
    colnames(agg)[2L] <- "n1"
    ## na.last=FALSE is a capitulation to Solr's failure to handle NAs well
    agg <- agg[order(agg$n1, na.last=FALSE),]
    rownames(agg) <- NULL
    checkIdentical(stats, agg)

### CHECK: more stats
    checkStats <- function(na.rm, ...) {
        if (!missing(...)) {
            query <- subset(query, ...)
            df <- subset(df, ...)
        }
        facet.query <- facet(query, ~ cat_s,
                             min(num_d, na.rm=.(na.rm)),
                             max(num_d, na.rm=.(na.rm)),
                             nunique(num_d, na.rm=.(na.rm)),
                             quantile(num_d, na.rm=.(na.rm)),
                             median(num_d, na.rm=.(na.rm)),
                             IQR(num_d, na.rm=.(na.rm)),
                             weighted.mean(num_d, abs(num_i), na.rm=.(na.rm)),
                             range(num_d, na.rm=.(na.rm)),
                             mean(num_d, na.rm=.(na.rm)),
                             any(val_b, na.rm=.(na.rm)),
                             all(val_b, na.rm=.(na.rm)))
        stats <- stats(facets(sc, facet.query)[[1L]])

        quantile2 <- function(x, prob=seq(0, 1, 0.25), na.rm=FALSE) {
            if (!na.rm & anyNA(x)) {
                setNames(rep(NA, length(prob)), paste0(prob*100, "%"))
            } else {
                quantile(x, prob, na.rm=na.rm, names=TRUE, type=7)
            }
        }

        IQR2 <- function (x, na.rm = FALSE, type = 7)
            diff(quantile2(as.numeric(x), c(0.25, 0.75), na.rm = na.rm))

        sdf <- by(df, df$cat_s, function(x) {
                      with(x, data.frame(count=nrow(x),
                                         num_d.min=min(num_d, na.rm=na.rm),
                                         num_d.max=max(num_d, na.rm=na.rm),
                                         num_d.nunique=
                                             nunique(num_d, na.rm=na.rm),
                                         num_d.quantile=
                                             I(t(quantile2(num_d,
                                                           na.rm=na.rm))),
                                         num_d.median=
                                             median(num_d, na.rm=na.rm),
                                         num_d.IQR=IQR2(num_d, na.rm=na.rm),
                                         num_d.weighted.mean=
                                             weighted.mean(num_d, abs(num_i),
                                                           na.rm=na.rm),
                                         num_d.range=
                                             I(t(range(num_d, na.rm=na.rm))),
                                         num_d.mean=mean(num_d, na.rm=na.rm),
                                         val_b.any=any(val_b, na.rm=na.rm),
                                         val_b.all=all(val_b, na.rm=na.rm)))
                  })
        sdf <- cbind(cat_s=levels(factor(df$cat_s)), do.call(rbind, sdf))
        sdf$cat_s <- as.character(sdf$cat_s)
        rownames(sdf) <- NULL
        colnames(sdf$num_d.range) <- c("min", "max")
        ## str(stats)
        ## str(sdf)
        checkIdentical(stats, sdf)
    }

    checkStats(FALSE)
    checkStats(TRUE)

### CHECK: cut on dates
    dateBreaks <- seq(parseSolrDate('2001-01-01T00:00:00Z'),
                      parseSolrDate('2003-01-01T00:00:00Z'),
                      by="year")
    checkTable2(~ cut(date_dt, dateBreaks))

### CHECK: cut with options
    checkTable2(~ cut(num_d, seq(-5, 10, 5)))
    checkTable2(~ cut(num_d, seq(-5, 10, 5), right=FALSE))
    checkTable2(~ cut(num_d, seq(-5, 10, 5), include.lowest=TRUE))

### CHECK: xtabs empty bucket
    dff <- transform(df, cat_s = as.factor(cat_s))
    tab <- xtabs(~ cat_s, dff, subset=cat_s=="A")
    fct <- facets(sc, xtabs(~ cat_s, query, subset=cat_s=="A"))[[1L]]
    checkIdentical(as.table(fct), toTable(tab))
    
### CHECK: stats on empty bucket
    checkStats(FALSE, cat_s == "A")
    checkStats(TRUE, cat_s == "A")
    
### CHECK: dropping empty facet queries
    facet.query <- facet(query, ~ I(num_d > 100), sum(num_d))
    agg <- aggregate(num_d ~ I(num_d > 100), df, sum, na.action=na.pass)
    colnames(agg)[2L] <- "num_d.sum"
    agg[1] <- lapply(agg[1], unclass)
    stats <- stats(facets(sc, facet.query)[[1L]])
    stats$count <- NULL
    checkIdentical(stats, agg)
    
### CHECK: stats on entire dataset
    facet.query <- facet(query, NULL, sum(num_d))
    stats <- stats(facets(sc, facet.query))
    agg <- data.frame(count = nrow(df), num_d.sum = sum(df$num_d))
    checkIdentical(stats, agg)
    facet.query <- facet(subset(query, cat_s == "C"), NULL, sum(num_d))
    stats <- stats(facets(sc, facet.query))
    agg <- data.frame(count = integer(), num_d.sum = numeric())
    checkIdentical(stats, agg)
    
### CHECK: drop=FALSE not allowed when stats given
    checkException(facet(query, ~ cat_s, sum(num_d), drop=FALSE))

### CHECK: mad(), unfortunately na.rm=FALSE does not work yet (with NAs)
    facet.query <- facet(query, NULL, mad(num_d, na.rm=TRUE))
    stats <- stats(facets(sc, facet.query))
    checkIdentical(stats$num_d.mad, mad(df$num_d, na.rm=TRUE))
}
