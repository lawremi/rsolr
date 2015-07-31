\name{SolrQuery-class}
\docType{class}

\alias{class:SolrQuery}
\alias{SolrQuery-class}

% Accessors:
\alias{params}
\alias{params<-}

% Constructor:
\alias{SolrQuery}

% Restriction and manipulation:
\alias{rename,SolrQuery-method}
\alias{subset,SolrQuery-method}
\alias{transform,SolrQuery-method}
\alias{sort,SolrQuery-method}
\alias{xtabs,SolrQuery-method}
\alias{facet,SolrQuery,NULL-method}
\alias{facet,SolrQuery,formula-method}
\alias{facet,SolrQuery,character-method}
\alias{head.SolrQuery}
\alias{tail.SolrQuery}
\alias{head,SolrQuery-method}
\alias{tail,SolrQuery-method}
\alias{window.SolrQuery}
\alias{window,SolrQuery-method}
\alias{rev,SolrQuery-method}

% Faceting:
\alias{facet}
\alias{facet,SolrQuery,NULL-method}

% Grouping:
\alias{group,SolrQuery,language-method}
\alias{group,SolrQuery,name-method}
\alias{group,SolrQuery,character-method}
\alias{group,SolrQuery,formula-method}

% Searching:
\alias{searchDocs,SolrQuery-method}

% Coercion:
\alias{as.character.SolrQuery}
\alias{as.character,SolrQuery-method}
\alias{translate,SolrQueryTranslationSource,Expression-method}
\alias{translate,SolrQueryTranslationSource,SolrQParserExpression-method}
\alias{translate,SolrQueryTranslationSource,SolrSortExpression-method}
\alias{translate,SolrQuery,missing-method}
\alias{as.character,SolrQueryTranslationSource-method}

% Show:
\alias{show,SolrQuery-method}

\title{SolrQuery}
\description{
  The \code{SolrQuery} object represents a query to be sent to a
  \code{\linkS4class{SolrCore}}. This is a low-level interface to query
  construction but will not be useful to most users. The typical reason
  to directly manipulate a query would be to batch more operations than is
  possible with the high-level \code{SolrFrame}, e.g., combining
  multiple aggregations.
}

\details{
  A \code{SolrQuery} API borrows many of the same verbs from the base R
  API, including \code{\link{subset}}, \code{\link{transform}},
  \code{\link{sort}}, \code{\link{xtabs}}, \code{\link{head}},
  \code{\link{tail}}, \code{\link{rev}}, etc.

  The typical workflow is to construct a query, perform various
  manipulations, and finally retrieve a result by passing the query to a
  \code{SolrCore}, typically via the \code{docs} or \code{facets}
  functions.
}

\section{Accessors}{
  \describe{
    \item{}{
      \code{params(x), params(x) <- value}: Gets/sets the parameters of
      the query, which roughly correspond to the parameters of a Solr
      \dQuote{select} request. The only reason to manipulate the
      underlying query parameters is to either initiate a headache or to
      do something really tricky with Solr, which implies the former.
    }
  }
}

\section{Querying}{
  \describe{
    \item{}{\code{subset(x, subset, select, fields, select.from =
        character())}: Behaves like the base \code{\link{subset}}, with
      some extensions. The \code{fields} argument is exclusive with
      \code{select}, and should be a character vector of field names,
      potentially with wildcards. The \code{select.from} argument
      gives the names that are filtered by \code{select}, since
      \code{SolrQuery} is not associated with any \code{SolrCore}, and
      thus does not know the field set (in the future, we might use
      laziness to avoid this problem).
    }
    \item{}{\code{searchDocs(x, q)}: Performs a conventional document
      search using the query string \code{q}. The main difference to
      filtering (\code{subset}) is that (by default) Solr will order the
      result by score, i.e., how well each document matches the query.
    }
  }
}

\section{Constructor}{
  \describe{
    \item{}{
      \code{SolrQuery(expr)}:
      Constructs a new \code{SolrQuery} instance. If \code{expr} is
      non-missing, it is passed to \code{subset} and thus serves as an
      initial restriction.
    }
  }
}

\section{Faceting}{
  The Solr facet component counts documents and calculates statistics on
  a group-wise basis.

  \describe{
    \item{}{\code{facet(x, by, ..., useNA=FALSE, sort=NULL,
        decreasing=FALSE, limit=NA_integer_)}: Returns a query that will
        compute the number of documents in each group, where the
        grouping is given as \code{by}, typically a formula, or
        \code{NULL} for global aggregation. Arguments in \dots are
        quoted and should be expressions that summarize fields, or
        mathematical combinations of fields. The names of the statistics
        are taken from the argument names; if a name is omitted, a best
        guess is made from the expression. If \code{useNA} is
        \code{TRUE}, statistics and counts are computed for the bin
        where documents have a missing value for one the grouping
        variables. If \code{sort} is non-NULL, it should name a
        statistic by which the results should be sorted. This is mostly
        useful in conjunction if a \code{limit} is specified, so that
        only the top-N statistics are returned.

        The formula should consist of Solr field names, or calls that
        evaluate to logical and refer to one or more Solr fields. If the
        latter, the results are grouped by \code{TRUE}, \code{FALSE} and
        (optionally) \code{NA} for that term. As a special case, a term
        can be a call to \code{cut} on any numeric or date field, which
        will group by bin.
    }
  }
}

\section{Grouping}{
  The Solr grouping component causes results to be returned nested into
  groups. The main use case would be to restrict to the first or last N
  documents in each group. This functionality is \emph{not} related to
  aggregation; see \code{facet}.

  \describe{
    \item{}{\code{group(x, by, limit = .Machine$integer.max, offset =
        0L, env = emptyenv())}: Returns the grouping of \code{x}
        according to \code{by}, which might be a formula, or an
        expression that evaluates (within \code{env}) to a factor. The
        current sort specification applies within the groups, and any
        subsequent sorting applies to the groups themselves, by using
        the maximum value within the each group. Only the top
        \code{limit} documents, starting after the first \code{offset},
        are returned from each group. Restricting that limit is probably
        the main reason to use this functionality.
    }
  }
}

\section{Coercion}{
  These two functions are very low-level; users should almost never need
  to call these.
  
  \describe{
    \item{}{
      \code{translate(x, target, core)}: Translates the query \code{x}
      into the language of Solr, where \code{core} specifies the
      destination \code{SolrCore}. The \code{target} argument should be
      missing.
    }
    \item{}{
      \code{as.character(x)}:
      Converts the query into a string to be sent to Solr. Remember to
      translate first, if necessary.
    }
  }
}

\author{ Michael Lawrence }

\seealso{
  \code{\linkS4class{SolrFrame}}, the recommended high-level interface
  for interacting with Solr

  \code{\linkS4class{SolrCore}}, which gives an example of constructing
  and evaluating a query
}

\keyword{methods}
\keyword{classes}
