
nodes <- data.frame( id = c('1', '2', '3' ), parent = c( NA, '3', '2' ), color = c( NA, 'blue', 'red' ), weight = c( 1.2, 3.5, 3.4 ),
                     bool = c( TRUE, FALSE, FALSE ),
                     stringsAsFactors = FALSE )
edges <- data.frame( source = c( '1' ), target = c( '2' ), weight = c( 1.2 ), stringsAsFactors = FALSE )
#message( DataFrameToGraphML( nodes, edges, nodeAttrs = c( Color = 'color', 'weight', 'bool' ),
#                             edgeAttrs = c( 'weight' , 'df' ) ) )

rcpp_hello_world <- function(){
	.Call( "rcpp_hello_world", PACKAGE = "RGraphML" )
}

