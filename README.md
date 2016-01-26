Fork of jcelliot's turnpike. 

#### Installation Instructions:
1. [Download and install go](http://www.jeffduckett.com/blog/55096fe3c6b86364cef12da5/installing-go-1-4-2-on-ubuntu-(trusty)-14-04.html)

2. [Set GOPATH](https://golang.org/doc/code.html). This is where all the go code lives. 

> $ mkdir $HOME/work

> $ export GOPATH=$HOME/work

3. Download the node source code.

> go get github.com/exis-io/node

4. Start the node
> go run $GOPATH/src/github.com/exis-io/node/runner/main.go
