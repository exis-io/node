## About

"Rabric" is a working name. It is a NaaS (Network as a Service) which consists of the Riffle protocol, Fabric network, and Appliances. Together these serve to abstract and simplify network communication implementation. 

[[Riffle|Riffle]] is a tentative fork of the WAMP protocol. It provides message-based communication between any two parties over the Fabric irrespective of language, underlying connection, or platform. It has two notable features: high-level usage and the internalization of network communication best practices. Developers and applications interact directly with riffle. 

The [[Fabric|Fabric]] is a collection of running programs called [[Nodes|Node]] that route riffle traffic similar to IP routers. Like riffle, the design of the fabric takes best practices and enforces them implicitly. Riffle requires at least one node to be used in a meaningful way.

[[Appliances|Appliances]] are discrete software services that expose high-level functionality. They can be authored, deployed, and utilized by any developer on the fabric. [[Core Appliances|Appliances]] are used to configure the behavior of the fabric or to enable deployment administration. 

Find descriptions of all these components through the sidebar. 

## Why?

Many message-based, language agnostic platforms have been developed in the same vein as Rabric. See: CORBA, Crossbar.io, Oracle's implementation, etc (TODO.) These each have their own shortcomings:

1. Language Dependence- language lock-in restricts usage
2. Enterprise Offerings- expensive or not accessible to all developers
3. Complex- introduce their own idiosyncrasies or require networking expertise to deploy

Compared to Rabric:

1. Language Agnostic- usable on all commonly used languages and between any combination of languages
2. Open Source- anyone can run their own fabric
3. Simple- using riffle is as easy as calling local code

## Getting Started

1. [Download and install go](http://www.jeffduckett.com/blog/55096fe3c6b86364cef12da5/installing-go-1-4-2-on-ubuntu-(trusty)-14-04.html)

2. [Set GOPATH](https://golang.org/doc/code.html). This is where all the go code lives. 

> $ mkdir $HOME/work

> $ export GOPATH=$HOME/work

3. Download the node source code.

> go get github.com/ParadropLabs/node

4. Start the node
> go run $GOPATH/src/github.com/ParadropLabs/node/runner/main.go
