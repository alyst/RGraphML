#' Generate GraphML file from node and edges data.frame.
#' 
#' Generates GraphML file from the graph
#' defined by the node and edges data.frames.
#' 
#' @param nodes data.frame of the graph nodes
#' @param edges data.frame of the graph edges
#' @param node_id.column nodes data.frame column containing node IDs
#' @param parent_id.column optional nodes data.frame column containing ID of the parent node (NA if no parent)
#' @param source.column edges data.frame column of the edge source node IDs
#' @param target.column edges data.frame column of the edge target node IDs
#' @param node.attrs vector of names of node data.frame columns containing node attributes to export
#' @param edge.attrs vector of names of edge data.frame columns containing edge attributes to export
#' @param is.directed if the generated graph should be directed
#' 
#' @return character string with the graph definition in GraphML XML format
#' 
#' @details
#' The names of the elements in the node.attrs and edge.attrs, if any, would be the names of the attributes in the generated GraphML.
#' 
#' @examples
#' nodes <- data.frame(
#'  id = c('1', '2', '3' ),
#'  parent = c( NA, '3', '2' ),
#'  color = c( NA, 'blue', 'red' ),
#'  weight = c( 1.2, 3.5, 3.4 ),
#'  bool = c( TRUE, FALSE, FALSE ),
#'  stringsAsFactors = FALSE )
#' edges <- data.frame( source = c( '1' ), target = c( '2' ),
#'  weight = c( 1.2 ),
#'  stringsAsFactors = FALSE )
#' GraphML.generate( nodes, edges,
#'                   node.attrs = c( Color = 'color', 'weight', 'bool' ),
#'                   edge.attrs = c( 'weight' ) )
#' 
#' @references \url{http://graphml.graphdrawing.org/}
#' @useDynLib RGraphML
#' @export
GraphML.generate <- function(
    nodes, edges,
    node_id.column = "id", parent_id.column = "NA",
    source.column = "source", target.column = "target",
    node.attrs = character(), edge.attrs = character(),
    is.directed = FALSE
){
    DataFrameToGraphML( nodes, edges,
        nodeIdCol = node_id.column, parentIdCol = parent_id.column,
        sourceCol = source.column, targetCol = target.column,
        nodeAttrs = node.attrs, edgeAttrs = edge.attrs,
        isDirected = is.directed )
}
