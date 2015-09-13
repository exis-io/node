Fork of jcelliot's turnpike. 

To run, clone into your go working directory and run:
    
    go get $GOPATH/github.com/damouse/rabric
    go run $GOPATH/github.com/damouse/rabric/exec/main.go

Heads up: more steps may be required to get up and running than this. 

The `exec` directory contains starter scripts for the node as well as lightweight sandbox examples of python clients. 
It is not intended for developer usage. 

`tests` contains python tests. They're here to aid in blackbox testing of the node.

The `examples` directory contain golang examples for starting the node. For the time being these should be considered out of date. 