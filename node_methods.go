/*
 * This module implements handlers for methods that are registered by the node
 * itself.
 */

package node

func (node *node) HandleGetUsage(args []interface{}, kwargs map[string]interface{}, details map[string]interface{}) (result *CallResult) {
	counts := node.stats.GetUsage()
	result = &CallResult {
        Args: []interface{}{counts},
    }
	return
}

func (node *node) HandleEvictDomain(args []interface{}, kwargs map[string]interface{}, details map[string]interface{}) (result *CallResult) {
	domain, ok := args[0].(string)

	count := 0
	if ok {
		count = node.EvictDomain(domain)
	}

	result = &CallResult {
		Args: []interface{}{count},
	}
	return
}

func (node *node) HandleUnregisterAll(args []interface{}, kwargs map[string]interface{}, details map[string]interface{}) (result *CallResult) {
	endpoint, ok := args[0].(string)

	count := 0
	if ok {
		count = node.Dealer.UnregisterAll(URI(endpoint))
	}

	result = &CallResult {
		Args: []interface{}{count},
	}
	return
}

func (node *node) RegisterNodeMethods() {
    options := make(map[string]interface{}, 0)
    endpoint := string(node.agent.pdid + "/getUsage")
    node.agent.Register(endpoint, node.HandleGetUsage, options)

    options = make(map[string]interface{}, 0)
    endpoint = string(node.agent.pdid + "/evictDomain")
    node.agent.Register(endpoint, node.HandleEvictDomain, options)

    options = make(map[string]interface{}, 0)
    endpoint = string(node.agent.pdid + "/unregisterAll")
    node.agent.Register(endpoint, node.HandleUnregisterAll, options)
}
