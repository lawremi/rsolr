\name{ListSolrResult-class}
\docType{class}

\alias{class:SolrResult}
\alias{SolrResult-class}

\alias{class:ListSolrResult}
\alias{ListSolrResult-class}

% Accessors:
\alias{docs,ListSolrResult-method}
\alias{ndoc,ListSolrResult-method}
\alias{facets,ListSolrResult-method}
\alias{ngroup,ListSolrResult-method}
\alias{groupings,ListSolrResult-method}

% Coercion:

% Show:

\title{ListSolrResult}
\description{
  The \code{SolrResult} object represents the result of a Solr query and
  usually contains a collection of documents and/or facets. The default
  implementation, \code{ListSolrResult}, directly stores the canonical
  JSON response from Solr. It is usually obtained by
  \code{\link[=eval,SolrQuery,SolrCore-method]{eval}}uating a
  \code{SolrQuery} on a \code{SolrCore}, which most users will never do.
}

\section{Accessors}{
  Since \code{ListSolrResult} inherits from \code{list}, one can access
  the raw JSON fields directly through the ordinary list accessors. One
  should only directly manipulate the Solr response when extending
  rsolr/Solr at a deep level. Higher-level accessors are described below.
  
  \describe{
    \item{}{
      \code{docs(x)}: Returns the found documents as
      a \code{\linkS4class{DocList}}
    }
    \item{}{
      \code{ndoc(x)}: Returns the number of documents found
    }
    \item{}{
      \code{facets(x)}: Returns any computed \code{\linkS4class{Facets}}
    }
    \item{}{
      \code{groupings(x)}: If Solr was asked to group the documents in
      the response, this returns each \code{\linkS4class{Grouping}}
      (there can be more than one) in a list
    }
    \item{}{
      \code{ngroup(x)}: Returns the number of groups in each grouping
    }
  }
}

\author{ Michael Lawrence }

\seealso{
  \code{\link[=docs,SolrCore-method]{docs}} and
  \code{\link[=facets,SolrCore-method]{facets}} on \code{SolrCore} are
  more convenient and usually sufficient
}

\keyword{methods}
\keyword{classes}
