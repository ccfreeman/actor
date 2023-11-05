# actor
Provide a framework for wrapping a python class and exposing its methods and/or properties to external users through a FastAPI interface. Clients can also use these classes through `proxy` services, in which a client will set a connector type for the proxy to handle messaging (http or amqp messaging), and receive an object that has the full functionality of the decorated actor class. This provides for RPC capabilities across multiple communication protocols in a way that feels local for clients.

This framework is completely asynchronous.
