import hashlib
import bisect

class ConsistantHashRing:
    def __init__(self, num_replicas=3):
        self.ring = {}                      # Stores hash -> node mapping
        self.sorted_hashes = []             # Sorted list of hashes
        self.num_replicas = num_replicas    # Virtual nodes per server

    def hash(self, key):
        """ Generate consistant hash value for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node):
        """ Add node to the hash ring with virtual nodes """
        for i in range(self.num_replicas):
            virtual_node = f"{node}#{i}"   # Create a unique virtual node
            node_hash = self.hash(virtual_node)
            self.ring[node_hash] = node
            bisect.insort(self.sorted_hashes, node_hash)    # Insert while maintaining order

    def remove_node(self, node):
        """ Remove node to the hash ring """
        for i in range(self.num_replicas):
            virtual_node = f"{node}#{i}"
            node_hash = self.hash(virtual_node)
            if node_hash in self.ring:
                del self.ring[node_hash]
                self.sorted_hashes.remove(node_hash)
    
    def get_node(self, key):
        """ Find the node responsible for storing given key """
        if not self.ring:
            return None
        
        key_hash = self.hash(key)
        idx = bisect.bisect(self.sorted_hashes, key_hash)   # Find correct node position
        if idx == len(self.sorted_hashes):
            idx = 0
        return self.ring[self.sorted_hashes[idx]]

hash_ring = ConsistantHashRing()

# Adding nodes
hash_ring.add_node("NodeA")
hash_ring.add_node("NodeB")
hash_ring.add_node("NodeC")

# Assigning Keys
keys = ["Apple", "Bananas", "Mango", "Grapes", "Watermellon", "Oranges", "Apple", "Apple", "Apple"]

for key in keys:
    print(f"{key} is assigned to {hash_ring.get_node(key)}")

print("\nRemove node - NodeB\n")
hash_ring.remove_node("NodeB")

for key in keys:
    print(f"{key} is assigned to {hash_ring.get_node(key)}")