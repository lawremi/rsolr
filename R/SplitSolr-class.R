### =========================================================================
### SplitSolr objects
### -------------------------------------------------------------------------
###
### Persistently grouped data, modeled as a list, resulting from
### split(x, ~).
###
### Operations to support:
###
### [[: extract members of specific group, as a restriction of 'solr',
###     -- accepts multiple arguments for multi-factor groupings.
###     -- also need $, with(), eval()...
### s/v/lapply(x, fun): calls 'fun' on the implicitly grouped 'solr'
### as.list(x): converts to a list of restricted Solr instances
### as.data.frame(x): returns data sorted by grouping
### [,head,tail: returns a restricted Solr, except 2D case gets field,
###              sorted by and attached to the grouping.
### length: returns number of groups
### names: group names (interactions use "." as separator)
### aggregate: aggregates the elements, pass-through to grouped 'solr'
###
### Side note: if split() has an LHS, we generate the grouped Solr,
### then extract a single-column promise.
###

setClass("SplitSolr", representation(solr="Solr"))
