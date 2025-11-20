# Generates mock event data from DAG graph execution for testing purposes

import json
import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
from faker import Faker

@dataclass
class GraphVertex:
    vertex_id: int
    vertex_type: Dict[str, Any]
    is_remoted: bool = False
    inputs: List[Tuple[int, int]] = None  # List of (predecessor_vertex_id, predecessor_output_index)

    def __post_init__(self):
        if self.inputs is None:
            self.inputs = []
        if self.vertex_type is None:
            self.vertex_type = {}

class MockGraphEventGenerator:
    def __init__(self):
        self.fake = Faker()
        self.base_timestamp = int(datetime.now().timestamp() * 1_000_000)  # in microseconds
        self.current_timestamp = self.base_timestamp
        self.vertex_counter = 1
    
    def generate_timestamp(self) -> int:
        """Generate incrementing microsecond timestamp"""
        self.current_timestamp += random.randint(1_000, 10_000)  # Increment by 1-10 ms
        return self.current_timestamp

    def generate_uuid(self) -> str:
        """Generate UUID without dashes for correlation keys"""
        return str(uuid.uuid4()).replace('-', '')[:22] + random.choice(['aw', 'cg', 'Qg'])
    
    def generate_graph_key(self, graph_uuid: str, client_ref: str, batch_uuid: str) -> Dict[str, Any]:
        """Generate a graph key dictionary"""
        return {
            "uuid": graph_uuid,
            "ref": client_ref,
            "batch_uuid": batch_uuid
        }
    
    def create_base_key(self, graph_uuid: str, client_ref: str, batch_uuid: str, vertex_id: int, timestamp: int) -> Dict[str, Any]:
        """Create a base event key structure"""
        graph_key = self.generate_graph_key(graph_uuid, client_ref, batch_uuid)
        return {
            "timestamp": timestamp,
            "graph_key": self.generate_graph_key(graph_uuid, client_ref, batch_uuid),
            "vertex_key": {"vertex_id": vertex_id, "version": 0}
        }

    def generate_payload_type(self, type_name: str, namespace: str = "test") -> Dict[str, str]:
        return {"name": type_name, "namespace": namespace}
    
    def generate_payload_value(self, payload_type: Dict[str, str]) -> Dict[str, Any]:
        return {
            "uid": None,
            "payloadType": payload_type,
            "class": None,
            "binary": None,
            "size": None,
            "uidCalculationDuration": None,
            "serializationDuration": None,
            "variant": {
                "type": "HANDLE"
            },
            "toString": self.fake.text(max_nb_chars=20) #  TODO: use predefined string for the toString values
        }
    
    def generate_vertex_type(self, payload_type: Dict[str, str]) -> Dict[str, Any]:
        """Each vertex and edge encapsulate an instance of a variant type and this method generates the variant type details"""
        return {
            # SHA-256 hash of the serialized instance of this variant type
            "uid": None,
            "type": payload_type,
            "class": None,  # TODO: get random class name from predefined list
            "size": None,  # TODO: generate random size
            
            "uid_generation_duration": None,
            # time taken to serialize instance of this variant type
            "serialization_duration": None,
            "variant": {
                "type": "HANDLE"
            },
            "toString": self.fake.text(max_nb_chars=20) #  TODO: use predefined string for the toString values
        }
    
    def emit_graph_accepted(self, graph_uuid: str, client_ref: str, batch_uuid: str) -> Dict[str, Any]:
        """Emit GRAPH_ACCEPTED event"""
        timestamp = self.generate_timestamp()
        return {
            "type": "GRAPH_ACCEPTED",
            "event": {
                "key": {
                    "instant": {"timestamp": timestamp},
                    "key": self.generate_graph_key(graph_uuid, client_ref, batch_uuid)
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
    
    def emit_graph_completed(self, graph_uuid: str, client_ref: str, batch_uuid: str) -> Dict[str, Any]:
        """Emit GRAPH_COMPLETED event"""
        timestamp = self.generate_timestamp()
        return {
            "type": "GRAPH_COMPLETED",
            "event": {
                "key": {
                    "instant": {"timestamp": timestamp},
                    "key": self.generate_graph_key(graph_uuid, client_ref, batch_uuid)
                }
            }
        }
    
    def emit_vertex_accepted(self, graph_uuid: str, client_ref: str, batch_uuid: str, vertex: GraphVertex) -> Dict[str, Any]:
        """Emit VERTEX_ACCEPTED event"""
        timestamp = self.generate_timestamp()
        return {
            "type": "VERTEX_ACCEPTED",
            "event": {
                "key": self.create_base_key(graph_uuid, client_ref, batch_uuid, vertex.vertex_id, timestamp),
                "type": vertex.vertex_type,
                "remoted": vertex.is_remoted,
                "remoted_by": "CLIENT_SUPPLIED_HOOK" if vertex.is_remoted else None,
                "terminal_vertex": vertex.vertex_id == 0
            }
        }