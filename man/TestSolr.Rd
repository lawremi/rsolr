\name{TestSolr}
\alias{TestSolr}
\title{Testing Solr}

\description{
  Launches an instance of the embedded Solr and creates a core for
  testing and demonstration purposes.
}

\usage{
TestSolr(schema = NULL, start = TRUE)
}

\arguments{
  \item{schema}{
    The \code{\linkS4class{SolrSchema}} object describing the schema for
    the new Solr core
  }
  \item{start}{
    Whether to actually start the server (it can be started later by
    interacting with the returned object)
  }
}
\value{
  An instance of \code{ExampleSolr}, a reference class. Typically, one
  just accesses the \code{uri} field, and passes it to a constructor of
  \code{SolrFrame} or \code{SolrCore}.
}
\author{
  Michael Lawrence
}
