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
    vertex_type: str
    is_remoted: bool = False
    inputs: List[Tuple[int, int]] = None  # List of (predecessor_vertex_id, predecessor_output_index)

    def __post_init__(self):
        if self.inputs is None:
            self.inputs = []

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
            "client_ref": client_ref,
            "batch_uuid": batch_uuid
        }
    
    def create_base_key(self, graph_uuid: str, client_ref: str, batch_uuid: str, vertex_id: int, timestamp: int) -> Dict[str, Any]:
        """Create a base event key structure"""
        graph_key = self.generate_graph_key(graph_uuid, client_ref, batch_uuid)
        return {
            "instant": {"espochMicrosecondsUTC": timestamp},
            "calcGraphCalculationKey": self.generate_graph_key(graph_uuid, client_ref, batch_uuid),
            "calcStepCalculationKey": {"vertexId": vertex_id, "version": 0}
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
            "toString": self.fake.text(max_nb_chars=20)
        }