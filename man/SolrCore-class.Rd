\name{SolrCore-class}
\docType{class}

\alias{class:SolrCore}
\alias{SolrCore-class}

% Accessors:
\alias{name,SolrCore-method}
\alias{ndoc,SolrCore-method}
\alias{schema}
\alias{schema,SolrCore-method}
\alias{fieldNames,SolrCore-method}
\alias{version}
\alias{version,SolrCore-method}

% Constructor:
\alias{SolrCore}

% Reading:
\alias{docs}
\alias{docs,SolrCore-method}
\alias{read,SolrCore-method}

% Evaluation
\alias{eval,SolrQuery,SolrCore-method}
\alias{eval,TranslationRequest,SolrCore-method}

% Summarizing:
\alias{facets}
\alias{facets,SolrCore-method}
\alias{groupings}
\alias{groupings,SolrCore-method}
\alias{ngroup,SolrCore-method}

% Updating:
\alias{update,SolrCore-method}
\alias{delete,SolrCore-method}
\alias{commit}
\alias{commit,SolrCore-method}
\alias{purgeCache,SolrCore-method}

% Show:
\alias{show,SolrCore-method}

\title{SolrCore}
\description{
  The \code{SolrCore} object represents a core hosted by a Solr
  instance. A core is essentially a queryable collection of documents
  that share the same schema. It is usually not necessary to interact
  with a \code{SolrCore} directly.
}

\details{
  The typical usage (by advanced users) would be to construct a custom
  \code{\linkS4class{SolrQuery}} and execute it via the \code{docs},
  \code{facets} or (the very low-level) \code{eval} methods.
}

\section{Accessor methods}{
  In the code snippets below, \code{x} is a \code{SolrCore} object.

  \describe{
    \item{}{\code{name(x)}: Gets the name of the core (specified by the
      schema).
    }
    \item{}{\code{ndoc(x, query = SolrQuery())}: Gets the number of
      documents in the core, given the \code{query} restriction.
    }
    \item{}{\code{schema(x)}: Gets the \code{\linkS4class{SolrSchema}}
      satisfied by all documents in the core.
    }
    \item{}{\code{fieldNames(x, query = NULL, onlyStored = FALSE,
        onlyIndexed = FALSE, includeStatic = FALSE)}: Gets the field
      names, given any restriction and/or transformation in
      \code{query}, which is a \code{SolrQuery} or a character vector of
      field patterns. The \code{onlyIndexed} and \code{onlyStored}
      arguments restrict the fields to those indexed and stored,
      respectively (see \code{\linkS4class{FieldInfo}} for more
      details). Setting \code{includeStatic} to \code{TRUE} ensures
      that all of the static fields in the schema are returned.
    }
    \item{}{\code{version(x)}: Gets the version of the Solr instance
      hosting the core.
    }
  }
}

\section{Constructor}{
  \describe{
    \item{}{
      \code{SolrCore(uri, ...)}:
      Constructs a new \code{SolrCore} instance, representing a Solr
      core located at \code{uri}, which should be a string or a
      \code{\link[restfulr]{RestUri}} object. If a string, then the
      \dots are passed to the \code{RestUri} constructor.
    }
  }
}

\section{Reading}{
  \describe{
    \item{}{
      \code{docs(x, query = SolrQuery(), as=c("list", "data.frame"))}:
      Get the documents selected by \code{query}, in the form indicated
      by \code{as}, i.e., either a list or a data frame.
    }
    \item{}{
      \code{read(x, ...)}: Just an alias for \code{docs}.
    }
  }
}

\section{Summarizing}{
  \describe{
    \item{}{
      \code{facets(x, by, ...)}:
      Gets the \code{\linkS4class{Facets}} results as requested by
      \code{by}, a \code{\linkS4class{SolrQuery}}. The \dots are passed
      down to \code{facets} on \code{\linkS4class{ListSolrResult}}.
    }
    \item{}{
      \code{groupings(x, by, ...)}:
      Gets the list of \code{\linkS4class{Grouping}} objects as requested by
      the grouped query \code{by}. The \dots are passed
      down to \code{groupings} on \code{\linkS4class{ListSolrResult}}.
    }
    \item{}{
      \code{ngroup(x)}: Gets the number of groupings that would be
      returned by \code{groupings}.
    }
  }
}

\section{Updating}{
  \describe{
    \item{}{
      \code{update(object, value, commit = TRUE, atomic = FALSE, ...)}:
      Load the documents in \code{value} (typically a list or data
      frame) into the SolrCore given by \code{object}. If \code{commit}
      is \code{TRUE}, we request that Solr commit the changes to its
      index on disk, with arguments in \code{...} fine-tuning the commit
      (see \code{commit}). If \code{atomic} is \code{TRUE}, then the
      existing documents are modified, rather than replaced, by the
      documents in \code{value}.
    }
    \item{}{
      \code{delete(x, which = SolrQuery(), ...)}:
      Deletes the documents specified by \code{which} (all by default),
      where the \dots are passed down to \code{update}.
    }
    \item{}{
      \code{commit(x, waitSearcher=TRUE, softCommit=FALSE,
        expungeDeletes=FALSE, optimize=TRUE, maxSegments=if (optimize) 1L)}:
      Commits the changes to the Solr index; see the Solr documentation
      for the meaning of the parameters.
    }
    \item{}{
      \code{purgeCache(x)}: Purges the client-side HTTP cache, which is
      useful if the Solr instance is using expiration-based HTTP caching
      and one needs to see the result of an update immediately.
    }
  }
}

\section{Evaluation}{
  \describe{
    \item{}{
      \code{eval(expr, envir, enclos)}:
      Evaluates the query \code{expr} in the core \code{envir},
      ignoring \code{enclos}. Unless otherwise requested by the query
      response type, the result should be returned as a
      \code{\linkS4class{ListSolrResult}}. 
    }
  }
}

\section{Coercion}{
  \describe{
    \item{}{
      \code{as.data.frame(x, row.names=NULL, optional=FALSE, ...)}:
    }
  }
}

\author{ Michael Lawrence }

\seealso{
  \code{\linkS4class{SolrFrame}}, the typical way to interact with a
  Solr core.
}

\examples{
\donttest{
     solr <- TestSolr()
     sc <- SolrCore(solr$uri)
     name(sc)
     ndoc(sc)

     delete(sc)
     
     docs <- list(
        list(id="2", inStock=TRUE, price=2, timestamp_dt=Sys.time()),
        list(id="3", inStock=FALSE, price=3, timestamp_dt=Sys.time()),
        list(id="4", price=4, timestamp_dt=Sys.time()),
        list(id="5", inStock=FALSE, price=5, timestamp_dt=Sys.time())
     )
     update(sc, docs)

     q <- SolrQuery(id \%in\% as.character(2:4))
     read(sc, q)

     solr$kill()
}
}

\keyword{methods}
\keyword{classes}
