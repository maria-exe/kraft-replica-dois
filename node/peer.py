import sys, os, grpc

generated_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "clusters"))
if generated_path not in sys.path: sys.path.insert(0, generated_path)

import cluster_pb2_grpc

class Peer:
    def __init__(self, node_id, host, port):
        self.node_id = node_id

        address = f"{host}:{port}"

        self.channel = grpc.insecure_channel(address) # cria o canal grpc
        self.stub = cluster_pb2_grpc.ClusterServiceStub(self.channel) # cria o stub

    def request_vote(self, request): # envia uma solicitacao de voto para outro no
        return self.stub.RequestVote(request, timeout=2)

    def close(self):
        self.channel.close()

    def fetch(self, request):
        return self.stub.Fetch(request, timeout=2)