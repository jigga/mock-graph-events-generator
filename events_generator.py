# Generates mock event data from DAG graph execution for testing purposes

import argparse
import json
import uuid
import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import asdict, dataclass, field
from collections import defaultdict
from faker import Faker
from secrets import token_hex
import os

def load_values_from_csv(file_path: str) -> List[Tuple[str, str]]:
    """Loads values from a CSV file."""
    values = []
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            values.append(tuple(row))
    return values

script_dir = os.path.dirname(__file__)
vertex_values_csv_path = os.path.join(script_dir, 'vertex_values.csv')
vertex_values = load_values_from_csv(vertex_values_csv_path)
remoted_vertices: List[Tuple[str, str]] = random.sample(vertex_values, k=33)

edge_values_csv_path = os.path.join(script_dir, 'edge_values.csv')
edge_values = load_values_from_csv(edge_values_csv_path)


# Global Faker instance
fake = Faker()

@dataclass
class Variant:
    uid: str
    type: str
    size: int
    to_string: str
    serialization_duration: Optional[int] = None

    @classmethod
    def random_vertex_value(cls) -> "Variant":
        uid = token_hex(11) # 11 bytes -> 22 hex chars
        (type, to_string) = random.choice(vertex_values)
        return cls(uid=uid, type=type, size=random.randint(100, 100_000), to_string=to_string)
    
    @classmethod
    def random_edge_value(cls) -> "Variant":
        uid = token_hex(11) # 11 bytes -> 22 hex chars
        (type, to_string) = random.choice(edge_values)
        size=random.randint(1024, 104_857_600)
        serialization_duration=random.randint(1_000, 1_000_000)
        return cls(uid=uid, type=type, size=size, to_string=to_string, serialization_duration=serialization_duration)

@dataclass
class GraphKey:
    id: str
    ref: str
    batch_id: str

    @classmethod
    def random(cls) -> "GraphKey":
        """Generate a random graph key string"""
        id = str(uuid.uuid4())
        ref = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=12))
        batch_id = str(uuid.uuid4())
        return cls(id=id, ref=ref, batch_id=batch_id)

@dataclass
class VertexKey:
    vertex_id: int
    version: int = 0

@dataclass
class GraphVertex:
    vertex_id: int
    value: Variant = None
    is_remoted: bool = False
    inputs: List[Tuple[int, int]] = field(default_factory=list)  # List of (predecessor_vertex_id, predecessor_output_index)

    @classmethod
    def create(cls, vertex_id:int) -> "GraphVertex":
        value = Variant.random_vertex_value()
        is_remoted = value.type in [t for (t, _) in remoted_vertices]
        return cls(vertex_id=vertex_id, value=value, is_remoted=is_remoted)

@dataclass
class GraphEdge:
    from_vertex_id: int
    from_output_index: int
    to_vertex_id: int
    to_input_index: int
    edge_type: Dict[str, Any]

@dataclass
class VariantType:
    name: str
    namespace: str = "test"

class MockGraphEventGenerator:
    def __init__(self):
        self.fake = Faker()
        self.base_timestamp = int(datetime.now().timestamp() * 1_000_000)  # in microseconds
        self.current_timestamp = self.base_timestamp
        self.vertex_counter = 1
    
    def generate_timestamp(self) -> int:
        """Generate incrementing microsecond timestamp"""
        self.current_timestamp += random.randint(10_000, 1_000_000)  # Increment by 10 ms - 1 s
        return self.current_timestamp

    def generate_uuid(self) -> str:
        """Generate UUID without dashes for correlation keys"""
        return str(uuid.uuid4()).replace('-', '')[:22] + random.choice(['aw', 'cg', 'Qg'])
    
    def create_base_key(self, graph_key: GraphKey, vertex_id: int, timestamp: int) -> Dict[str, Any]:
        """Create a base event key structure"""
        return {
            "timestamp": timestamp,
            "graph_key": asdict(graph_key),
            "vertex_key": asdict(VertexKey(vertex_id))
        }
    
    def emit_graph_accepted(self, graph_key: GraphKey) -> Dict[str, Any]:
        """Emit GRAPH_ACCEPTED event"""
        timestamp = self.generate_timestamp()
        return {
            "type": "GRAPH_ACCEPTED",
            "event": {
                "key": {
                    "instant": {"timestamp": timestamp},
                    "key": asdict(graph_key)
                },
                "process": {
                    "id": random.randint(1000, 9999),
                    "memory": self.fake.random_number(digits=7),
                    "user": self.fake.user_name(),
                    "host": self.fake.hostname()
                },
                "engine": {
                    "version": "1.0.0-mock",
                },
                "settings": {
                    "features": [], #  TODO: get random feature flags from a predefined list
                    "hooks": {
                        "cache": "",
                        "distribution": "",
                        "expansion": ""
                    }
                }
            }
        }
    
    def emit_graph_completed(self, graph_key: GraphKey) -> Dict[str, Any]:
        """Emit GRAPH_COMPLETED event"""
        timestamp = self.generate_timestamp()
        return {
            "type": "GRAPH_COMPLETED",
            "event": {
                "key": {
                    "instant": {"timestamp": timestamp},
                    "key": asdict(graph_key)
                }
            }
        }
    
    def emit_vertex_accepted(self, graph_key: GraphKey, vertex: GraphVertex) -> Dict[str, Any]:
        """Emit VERTEX_ACCEPTED event"""
        timestamp = self.generate_timestamp()
        return {
            "type": "VERTEX_ACCEPTED",
            "event": {
                "key": self.create_base_key(graph_key, vertex.vertex_id, timestamp),
                "type": vertex.value.type,
                "remoted": vertex.is_remoted,
                "remoted_by": "CLIENT_SUPPLIED_HOOK" if vertex.is_remoted else None,
                "terminal_vertex": vertex.vertex_id == 0
            }
        }
    
    def emit_edge_accepted(self, graph_key: GraphKey, vertex_id: int, input_index: int, predecessor_vertex_id: int, predecessor_output_index: int) -> Dict[str, Any]:
        """Emit EDGE_ACCEPTED event"""
        timestamp = self.generate_timestamp()
        key = self.create_base_key(graph_key, vertex_id, timestamp)
        key.update({
            "index": input_index,
            "predecessor_key": {
                "vertex_id": predecessor_vertex_id,
                "index": predecessor_output_index
            }
        })
        return {
            "type": "EDGE_ACCEPTED",
            "event": {
                "key": key
            }
        }
    
    def emit_edge_calculated(self, graph_key: GraphKey, vertex_id: int, input_index: int, predecessor_vertex_id: int, predecessor_output_index: int) -> Dict[str, Any]:
        """Emit EDGE_CALCULATED event"""
        timestamp = self.generate_timestamp()
        
        key = self.create_base_key(graph_key, vertex_id, timestamp)
        key.update({
            "index": input_index,
            "predecessor_key": {
                "vertex_id": predecessor_vertex_id,
                "index": predecessor_output_index
            }
        })
        payload_value = asdict(Variant.random_edge_value())
        return {
            "type": "EDGE_CALCULATED",
            "event": {
                "key": key,
                "value": payload_value
            }
        }
    
    def emit_vertex_scheduled(self, graph_key: GraphKey, vertex: GraphVertex) -> Dict[str, Any]:
        """Emit VERTEX_SCHEDULED event"""
        timestamp = self.generate_timestamp()
        return {
            "type": "VERTEX_SCHEDULED",
            "event": {
                "key": self.create_base_key(graph_key, vertex.vertex_id, timestamp)
            }
        }
    
    def emit_remoted_vertex_accepted(self, graph_key: GraphKey, vertex: GraphVertex, correlation_id: str) -> Dict[str, Any]:
        """Emit REMOTED_VERTEX_ACCEPTED event"""
        timestamp = self.generate_timestamp()
        return {
            "type": "REMOTED_VERTEX_ACCEPTED",
            "event": {
                "key": self.create_base_key(graph_key, vertex.vertex_id, timestamp),
                "correlation_key": {
                    "correlation_id": correlation_id
                }
            }
        }
    
    def emit_vertex_invoked(self, graph_key: GraphKey, vertex: GraphVertex, correlation_id: str) -> Dict[str, Any]:
        """Emit VERTEX_INVOKED event"""
        timestamp = self.generate_timestamp()
        return {
            "type": "VERTEX_INVOKED",
            "event": {
                "key": self.create_base_key(graph_key, vertex.vertex_id, timestamp),
                "process_info": {
                    "id": random.randint(1000, 9999),
                    "memory": self.fake.random_number(digits=7),
                    "user": self.fake.user_name(),
                    "host": self.fake.hostname()
                },
                "correlation_key": {
                    "correlation_id": correlation_id
                }
            }
        }
    
    def emit_vertex_calculated(self, graph_key: GraphKey, vertex: GraphVertex, correlation_id: str) -> Dict[str, Any]:
        """Emit VERTEX_CALCULATED event"""
        timestamp = self.generate_timestamp()
        
        return {
            "type": "VERTEX_CALCULATED",
            "event": {
                "key": self.create_base_key(graph_key, vertex.vertex_id, timestamp),
                "correlation_key": {
                    "correlation_id": correlation_id
                }
            }
        }
    
    def emit_edge_deserialization_accepted(self, graph_key: GraphKey, vertex_id: int, input_index: int, size: int) -> Tuple[Dict[str, Any], str]:
        """Emit EDGE_DESERIALIZATION_ACCEPTED event"""
        timestamp = self.generate_timestamp()
        correlation_id = self.generate_uuid()
        
        key = self.create_base_key(graph_key, vertex_id, timestamp)
        key.update({
            "index": input_index,
            "predecessor_key": asdict(VertexKey(-1, -1))
        })
        
        return {
            "type": "EDGE_DESERIALIZATION_ACCEPTED",
            "event": {
                "key": key,
                "size": size,
                "correlation_key": {
                    "correlation_id": correlation_id
                }
            }
        }, correlation_id
    
    def emit_edge_deserialization_completed(self, graph_key: GraphKey, vertex_id: int, input_index: int, correlation_id: str) -> Dict[str, Any]:
        """Emit EDGE_DESERIALIZATION_COMPLETED event"""
        timestamp = self.generate_timestamp()
        
        key = self.create_base_key(graph_key, vertex_id, timestamp)
        key.update({
            "index": input_index,
            "predecessor_key": asdict(VertexKey(-1, -1))
        })
        
        return {
            "type": "EDGE_DESERIALIZATION_COMPLETED",
            "event": {
                "key": key,
                "correlation_key": {
                    "correlation_id": correlation_id
                }
            }
        }
    
    def emit_vertex_serialization_accepted(self, graph_key: GraphKey, vertex: GraphVertex) -> Tuple[Dict[str, Any], str]:
        """Emit VERTEX_SERIALIZATION_ACCEPTED event"""
        timestamp = self.generate_timestamp()
        correlation_id = self.generate_uuid()

        return {
            "type": "VERTEX_SERIALIZATION_ACCEPTED",
            "event": {
                "key": self.create_base_key(graph_key, vertex.vertex_id, timestamp),
                "correlation_key": {
                    "correlation_id": correlation_id
                }
            }
        }, correlation_id

    def emit_vertex_serialization_completed(self, graph_key: GraphKey, vertex: GraphVertex, correlation_id: str) -> Dict[str, Any]:
        """Emit VERTEX_SERIALIZATION_COMPLETED event"""
        timestamp = self.generate_timestamp()

        return {
            "type": "VERTEX_SERIALIZATION_COMPLETED",
            "event": {
                "key": self.create_base_key(graph_key, vertex.vertex_id, timestamp),
                "value": asdict(vertex.value),
                "correlation_key": {
                    "correlation_id": correlation_id
                }
            }
        }

    def emit_edge_serialization_accepted(self, graph_key: GraphKey, vertex_id: int, input_index: int, predecessor_vertex_id: int, predecessor_output_index: int) -> Tuple[Dict[str, Any], str]:
        """Emit EDGE_SERIALIZATION_ACCEPTED event"""
        timestamp = self.generate_timestamp()
        correlation_id = self.generate_uuid()
        key = self.create_base_key(graph_key, vertex_id, timestamp)
        key.update({
            "index": input_index,
            "predecessor_key": {
                "vertex_id": predecessor_vertex_id,
                "index": predecessor_output_index
            }
        })
        return { "type": "EDGE_SERIALIZATION_ACCEPTED", "event": { "key": key, "correlation_key": { "correlation_id": correlation_id } } }, correlation_id

    def emit_edge_serialization_completed(self, graph_key: GraphKey, vertex_id: int, input_index: int, predecessor_vertex_id: int, predecessor_output_index: int, correlation_id: str) -> Dict[str, Any]:
        """Emit EDGE_SERIALIZATION_COMPLETED event"""
        timestamp = self.generate_timestamp()
        key = self.create_base_key(graph_key, vertex_id, timestamp)
        key.update({
            "index": input_index,
            "predecessor_key": {
                "vertex_id": predecessor_vertex_id,
                "index": predecessor_output_index
            }
        })
        payload_value = asdict(Variant.random_edge_value())
        return { "type": "EDGE_SERIALIZATION_COMPLETED", "event": { "key": key, "value": payload_value, "correlation_key": { "correlation_id": correlation_id } } }

    def generate_mock_graph_execution(self, num_vertices: int = 5) -> List[Dict[str, Any]]:
        """Generate a mock graph execution with events"""
        graph_key = GraphKey.random()

        events = []
        vertices: List[GraphVertex] = []

        # Emit GRAPH_ACCEPTED
        events.append(self.emit_graph_accepted(graph_key))
        
        # Create graph topology
        for i in range(num_vertices):
            vertex = GraphVertex.create(vertex_id=i)
            inputs: List[Tuple[int, int]] = []
            if i > 0:
                num_inputs = random.randint(1, min(3, i))
                for _ in range(num_inputs):
                    pred_vertex = random.choice(vertices[:i])
                    inputs.append((pred_vertex.vertex_id, 0))
            
            vertex.inputs = inputs
            vertices.append(vertex)
        
        # Find which vertices have remote successors
        is_predecessor_to_remoted = {i: False for i in range(num_vertices)}
        for vertex in vertices:
            if vertex.is_remoted:
                for pred_id, _ in vertex.inputs:
                    is_predecessor_to_remoted[pred_id] = True

        for vertex in vertices:
            # Emit VERTEX_ACCEPTED
            events.append(self.emit_vertex_accepted(graph_key, vertex))
            
            for input_index, (pred_vertex_id, pred_output_index) in enumerate(vertex.inputs):
                # Emit EDGE_ACCEPTED
                events.append(self.emit_edge_accepted(graph_key, vertex.vertex_id, input_index, pred_vertex_id, pred_output_index))

                if vertex.is_remoted:
                    # Emit edge serialization events if the current vertex is remote
                    edge_ser_accepted_event, edge_ser_corr_id = self.emit_edge_serialization_accepted(graph_key, vertex.vertex_id, input_index, pred_vertex_id, pred_output_index)
                    events.append(edge_ser_accepted_event)
                    events.append(self.emit_edge_serialization_completed(graph_key, vertex.vertex_id, input_index, pred_vertex_id, pred_output_index, edge_ser_corr_id))
                else:
                    events.append(self.emit_edge_calculated(graph_key, vertex.vertex_id, input_index, pred_vertex_id, pred_output_index))
            
            # Emit VERTEX_SCHEDULED
            events.append(self.emit_vertex_scheduled(graph_key, vertex))
            
            correlation_id = self.generate_uuid()

            # For remoted vertices, we emit the following additional events:
            # REMOTED_VERTEX_ACCEPTED
            # EDGE_DESERIALIZATION_ACCEPTED
            # EDGE_DESERIALIZATION_COMPLETED
            if vertex.is_remoted:
                # Emit REMOTED_VERTEX_ACCEPTED
                events.append(self.emit_remoted_vertex_accepted(graph_key, vertex, correlation_id))
                
                for input_index in range(len(vertex.inputs)):
                    edge_value = Variant.random_edge_value()
                    # Emit EDGE_DESERIALIZATION_ACCEPTED
                    deserialization_accepted_event, edge_correlation_id = self.emit_edge_deserialization_accepted(
                        graph_key, vertex.vertex_id, input_index, edge_value.size)
                    events.append(deserialization_accepted_event)
                    
                    # Emit EDGE_DESERIALIZATION_COMPLETED
                    events.append(self.emit_edge_deserialization_completed(
                        graph_key, vertex.vertex_id, input_index, edge_correlation_id))
            
            # Emit VERTEX_INVOKED
            events.append(self.emit_vertex_invoked(graph_key, vertex, correlation_id))
            
            # Emit VERTEX_CALCULATED
            events.append(self.emit_vertex_calculated(graph_key, vertex, correlation_id))

            # If this vertex is a predecessor to a remoted vertex, its result needs to be serialized.
            if is_predecessor_to_remoted[vertex.vertex_id]:
                vertex_ser_accepted_event, vertex_ser_corr_id = self.emit_vertex_serialization_accepted(graph_key, vertex)
                events.append(vertex_ser_accepted_event)
                events.append(self.emit_vertex_serialization_completed(graph_key, vertex, vertex_ser_corr_id))
        
        # Emit GRAPH_COMPLETED
        events.append(self.emit_graph_completed(graph_key))
        
        return events

def main():
    parser = argparse.ArgumentParser(description="Generate mock graph execution events.")
    parser.add_argument('--num-vertices', type=int, help='Number of vertices in the graph.')
    args = parser.parse_args()

    if args.num_vertices:
        num_vertices = args.num_vertices
    else:
        num_vertices = random.randint(100, 10000)

    generator = MockGraphEventGenerator()
    events = generator.generate_mock_graph_execution(num_vertices=num_vertices)

    timestamp_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f"graph_events_{timestamp_suffix}.ndjson"

    with open(output_filename, 'w') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')

    print(f"Generated {len(events)} events for a graph with {num_vertices} vertices into {output_filename}")

if __name__ == "__main__":
    main()