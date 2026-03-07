# Things that need to be done once the rest is complete

## About how to run / start a workflow

The spec has 3 patterns defined currently:

```
# 1. Synchronous — blocks until complete
result = my_agent("Build a CLI tool")

# 2. Start — returns a handle for longer-running execution
handle = my_agent.start("Build a CLI tool")

# 3. Deploy — starts an execution on a named stack
handle = my_agent.deploy("Build a CLI tool", stack="aws-sandbox")
```

Hamza said the following:

result = my_agent("Build a CLI tool") -> lets get rid of this one
handle = my_agent.start("Build a CLI tool") -> this is subsumed by execute? or run. i think run is the common one that langgraph uses .. start and execute are probably not as common

## Config directory when logging in

- btw i can see that in phase 2 you have named the config directory still zenml, and we can see the active project which isnt a concept we've exposed in kitaru. 
- default project should be used and the config path either hidden or the name kitaru used

## 