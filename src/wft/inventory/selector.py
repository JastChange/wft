from .models import Inventory, Node, NodeSelector


def select_nodes(inventory: Inventory, selector: NodeSelector) -> tuple[Node, ...]:
    selected: list[Node] = []
    requested_groups = set(selector.groups)
    requested_tags = set(selector.tags)
    for node in inventory.nodes:
        matches = selector.all_enabled or node.name in selector.node_names
        matches = matches or bool(set(node.groups) & requested_groups)
        matches = matches or bool(set(node.tags) & requested_tags)
        if node.enabled and matches:
            selected.append(node)
    return tuple(sorted(selected, key=lambda node: node.name))
