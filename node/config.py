# enderecos de cada no (id e portas)
NODES = {
    1: ("localhost", 50051),
    2: ("localhost", 50052),
    3: ("localhost", 50053),
    4: ("localhost", 50054)
}
# retorna de todos os nos
def get_peers(node_id): 
    return {
        peer_id: address
        for peer_id, address in NODES.items()
        if peer_id != node_id
    }