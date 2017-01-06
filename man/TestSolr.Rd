\name{TestSolr}
\alias{TestSolr}
\title{Testing Solr}

\description{
  Launches an instance of the embedded Solr and creates a core for
  testing and demonstration purposes.
}

\usage{
TestSolr(schema = NULL, start = TRUE, restart = FALSE)
}

\arguments{
  \item{schema}{
    The \code{\linkS4class{SolrSchema}} object describing the schema for
    the new Solr core
  }
  \item{start}{
    Whether to actually start the server (it can be started later by
    interacting with the returned object). If there is already a server
    running, the return value points to that instance.
  }
  \item{restart}{
    Force the Solr server to restart.
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
