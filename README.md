# Description

The \`rsolr\` package is an idiomatic R interface to Solr based on
deferred evaluation.  A Solr core is represented as a data frame or
list that supports Solr-side filtering, sorting, transformation and
aggregation, all through the familiar base R API. Queries are
processed lazily, i.e., a query is only sent to the database when the
data are required.

# Features

-   Store, retrieve and compute on data with Solr, from R
-   Use familiar R syntax and function names from the base R API, with
    some extensions
-   Model data as either a `data.frame` or `list`, or use the
    low-level query API
-   Conveniently manipulate document collections after retrieval
-   Autogenerate a Solr schema from a `data.frame`
-   Experiment with the embedded Solr instance
-   Extend to support additional/custom Solr features

# Example using data from R

This is inspired by some manipulations in the `dplyr` vignette.

Load the New York City 2013 flight data and upload to Solr:

    library(nycflights13)
    schema <- deriveSolrSchema(flights)
    solr <- rsolr:::TestSolr(schema)
    sr <- SolrFrame(solr$uri)
    sr[] <- flights

Filtering:

    subset(sr, month == 1 & day == 1)
    head(sr, 10L)

Sorting:

    sort(sr, by = ~ year + month + day)

Select fields:

    subset(sr, select=c(year, month, day))
    sr[c("year", "month", "day")]
    sr[c("arr_*", "dep_*")] # Solr globs

Transform:

    sr2 <- transform(sr,
                     gain = arr_delay - dep_delay,
                     speed = distance / air_time * 60)
    sr2[c("gain", "speed")]

Aggregate:

    unique(sr["tailnum"])
    aggregate(~ tailnum, sr,
              count = TRUE,
              dist = mean(distance, na.rm=TRUE),
              delay = mean(arr_delay, na.rm=TRUE))

# Example using existing Solr core

Construct a SolrFrame using the URL to the existing core:

	sr <- SolrFrame("http://my.host.com/solr/mycore")
	
Convert the SolrFrame to a data.frame, typically after some filtering
or aggregation:

	df <- as.data.frame(sr)
