from collections import defaultdict
from heapq import heapify, heappop, heappush


class ReplayOrderError(ValueError):
    pass


def validate_predecessors(by_identity):
    for identity, revision in by_identity.items():
        for required in revision.requires:
            if required not in by_identity:
                raise ReplayOrderError(f'{identity.key} requires missing predecessor {required.key}')


def order_effective_events(revisions):
    by_identity = {}
    for item in revisions:
        prior = by_identity.setdefault(item.identity, item)
        if prior != item:
            raise ReplayOrderError(f'conflicting revision for {item.identity.key}')
    validate_predecessors(by_identity)

    by_field = defaultdict(dict)
    parents = {}
    for identity, item in by_identity.items():
        positions = by_field[identity.field_key]
        if item.seq in positions:
            raise ReplayOrderError(f'sequence conflict on {identity.field_key} at {item.seq}')
        positions[item.seq] = identity
        parents[identity] = set(item.requires)
    for positions in by_field.values():
        ordered = [positions[seq] for seq in sorted(positions)]
        for earlier, later in zip(ordered, ordered[1:]):
            parents[later].add(earlier)

    successors = defaultdict(list)
    for child, predecessors in parents.items():
        for predecessor in predecessors:
            successors[predecessor].append(child)

    def priority(identity):
        return (by_identity[identity].seq, identity.entity, identity.field,
                identity.source, identity.event_id, identity)

    ready = [priority(identity) for identity, edges in parents.items() if not edges]
    heapify(ready)
    ordered = []
    while ready:
        identity = heappop(ready)[-1]
        ordered.append(by_identity[identity])
        for successor in successors[identity]:
            parents[successor].remove(identity)
            if not parents[successor]:
                heappush(ready, priority(successor))
    if len(ordered) != len(by_identity):
        raise ReplayOrderError('explicit predecessors conflict with the effective order')
    return tuple(ordered)
