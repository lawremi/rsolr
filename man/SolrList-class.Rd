\name{SolrList-class}
\docType{class}

\alias{class:SolrList}
\alias{SolrList-class}

% Accessors:
\alias{length,SolrList-method}
\alias{names,SolrList-method}
\alias{[<-,SolrList,ANY,ANY,ANY-method}
\alias{[[,SolrList,ANY-method}
\alias{[,SolrList-method}
\alias{[[,SolrList-method}
\alias{[[<-,SolrList-method}

% Summarizing
\alias{unique,SolrList-method}

% Searching
\alias{searchDocs}
\alias{searchDocs,Solr}

% Constructor:
\alias{SolrList}

% Coercion:
\alias{coerce,Solr,SolrList-method}
\alias{coerce,SolrList,DocCollection-method}

% Show:
\alias{show,SolrList-method}

\title{SolrList}
\description{
  The \code{SolrList} object makes Solr data accessible through a
  list-like interface. This interface is appropriate when the data are
  highly ragged.
}

\details{
  A \code{SolrList} should more or less behave analogously to a list. It
  provides the same basic accessors (\code{\link{length}},
  \code{\link{names}}, \code{\link{[}}, \code{\link{[<-}},
  \code{\link{[[}}, \code{\link{[[<-}}, \code{\link{$}},
  \code{\link{$<-}}, \code{\link{head}}, \code{\link{tail}}, etc) and
  can be coerced to a list via \code{\link{as.list}}. Supported types of
  data manipulations include \code{\link{subset}},
  \code{\link{transform}}, \code{\link{sort}}, \code{\link{xtabs}},
  \code{\link{aggregate}}, \code{\link{unique}}, \code{\link{summary}},
  etc.

  An obvious difference between a \code{SolrList} and an ordinary list
  is that we know the \code{SolrList} contains only documents, which are
  themselves represented as named lists of fields, usually vectors of
  length one. This constraint enables us to provide the convenience of
  accessing fields by slicing across every document. We can pass a field
  selection to the second argument of \code{[}. Like data frame,
  selecting a single column with e.g. \code{x[,"foo"]} will return the
  field as a vector, filling NAs whereever a document lacks a
  value for the field.
  
  The names are taken from the field declared in the schema to
  represent the unique document key. Schemas are not strictly required
  to declare such a field, so if there is no unique key, the names
  are \code{NULL}.

  Field restrictions passed to e.g. \code{[} or \code{subset(fields=)}
  may be specified by name, or wildcard pattern (glob). Similarly, a row
  index passed to \code{[} must be either a character vector of
  identifiers (of length <= 1024, NAs are not supported, and this
  requires a unique key in the schema) or a
  \code{\linkS4class{SolrPromise}}/\code{\linkS4class{SolrExpression}},
  but note that if it evaluates to NAs, the corresponding rows are
  excluded from the result, as with \code{subset}. Using a
  \code{SolrPromise} or \code{SolrExpression} is recommended, as
  filtering happens at the database.
  
  A \code{SolrList} can be made lazy by calling \code{defer} on a
  \code{SolrList}, so that all column retrieval, e.g., via \code{[},
  returns a \code{\linkS4class{SolrPromise}} object. Many operations on
  promises are deferred, until they are finally \code{fulfill}ed by
  being shown or through explicit coercion to an R vector.

  A note for developers: \code{SolrFrame} and \code{SolrList} share
  common functionality through the base \code{Solr} class. Much of the
  functionality mentioned here is actually implemented as methods on the
  \code{Solr} class.
}

\section{Accessors}{
  These are some accessors that \code{SolrList} adds on top of the
  basic data frame accessors. Most of these are for advanced use only.
  
  \describe{
    \item{}{
      \code{ndoc(x)}: Gets the number of documents (rows); serves as an
      abstraction over \code{SolrFrame} and \code{SolrList}
    }
    \item{}{
      \code{nfield(x)}: Gets the number of fields (columns); serves as an
      abstraction over \code{SolrFrame} and \code{SolrList}
    }
    \item{}{
      \code{ids(x)}: Gets the document unique identifiers (may
      be \code{NULL}, treated as rownames); serves as an abstraction
      over \code{SolrFrame} and \code{SolrList}
    }
    \item{}{
      \code{fieldNames(x, ...)}: Gets the name of each field represented by
      any document in the Solr core, with \dots being passed down to
      \code{fieldNames} on \code{\linkS4class{SolrCore}}.
    }
    \item{}{
      \code{core(x)}: Gets the \code{SolrCore} wrapped by \code{x}
    }
    \code{}{
      \code{query(x)}: Gets the query that is being constructed by
      \code{x}
    }
  }
}

\section{Extended API}{
  Most of the typical data frame accessors and data manipulation
  functions will work analogously on \code{SolrList} (see
  Details). Below, we list some of the non-standard methods that might
  be seen as an extension of the data frame API.

  \describe{
    \item{}{\code{rename(x, ...)}: Renames the columns of \code{x},
      where the names and character values of \dots indicates the
      mapping (\code{newname = oldname}). 
    }
    \item{}{\code{defer(x)}: Returns a \code{SolrList} that yields
      \code{\linkS4class{SolrPromise}} objects instead of vectors
      whenever a field is retrieved
    }
    \item{}{\code{searchDocs(x, q)}: Performs a conventional document
      search using the query string \code{q}. The main difference to
      filtering is that (by default) Solr will order the result by
      score, i.e., how well each document matches the query.
    }
  }
}

\section{Constructor}{
  \describe{
    \item{}{
      \code{SolrList(uri, ...)}:
      Constructs a new \code{SolrList} instance, representing a Solr
      core located at \code{uri}, which should be a string or a
      \code{\link[restfulr]{RestUri}} object. The
      \dots are passed to the \code{SolrQuery} constructor.
    }
  }
}


\section{Evaluation}{
  \describe{
    \item{}{
      \code{eval(expr, envir, enclos)}: Evaluates R language \code{expr}
      in the \code{SolrList} \code{envir}, using \code{enclos} as the
      enclosing environment.
    }
  }
}

\section{Coercion}{
  \describe{
    \item{}{
      \code{as.data.frame(x, row.names=NULL, optional=FALSE, fill=FALSE)}:
      Downloads the data into an actual data.frame, specifically an
      instance of \code{\linkS4class{DocDataFrame}}. If \code{fill} is
      FALSE, only the fields represented in at least one document are
      added as columns. 
    }
    \item{}{
      \code{as.list(x), as(x, "DocCollection")}: Coerces \code{x} into
      the corresponding list, specifically an instance of
      \code{\linkS4class{DocList}}.
    }
  }
}

\author{ Michael Lawrence }

\seealso{
  \code{\linkS4class{SolrFrame}} for representing a Solr collection as a
  table instead of a list
}

\examples{
     solr <- TestSolr()
     sr <- SolrList(solr$uri)
     length(sr)
     head(sr)
     sr[["GB18030TEST"]]
     # Solr tends to crash for some reason running this inside R CMD check
     \dontrun{ 
     as.list(subset(sr, price > 100))[,"price"]
     }
     solr$kill()
}

\keyword{methods}
\keyword{classes}
