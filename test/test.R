require( RGraphML )

nodes.df <- data.frame( id = c(1,2,3,4,5),
                        weight = c( 0.5, 1.0, 0.2, 3, 2 ),
                        is_legal = c( TRUE, FALSE, TRUE, TRUE, TRUE ),
                        label = c( 'a', 'b', 'c', 'd', 'e' ),
                        counts = as.integer( c(1,2,3,4,5) ),
                        width = c( 5, 10, 10, 10, 5 ),
                        height = c( 5, 5, 3, 10, 5 ),
                        parent = c(NA,NA,NA,3,3),
                        stringsAsFactors = FALSE )
edges.df <- data.frame( id = c(1,2,3,4),
                        source = c(1,2,3,4),
                        target = c(2,3,1,5),
                        counts = as.integer(c(1,2,3,4)),
                        weight = c(1,2,3,2) )


tulip.txt <- tulip.generate( nodes.df, edges.df,
                             parent_id.column = 'parent',
                             node.attrs = c( 'weight', viewLabel = 'label',
                                             `viewSize[w]` = 'width',
                                             `viewSize[h]` = 'height',
                                             'is_legal', 'counts' ),
                             edge.attrs = c('weight','counts','target') )
message(tulip.txt)
write( tulip.txt, file = '~/test.tlp' )
