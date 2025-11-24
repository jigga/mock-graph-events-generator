# Generates mock event data from DAG graph execution for testing purposes

import json
import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import asdict, dataclass, field
from collections import defaultdict
from faker import Faker
from secrets import token_hex

vertex_values: List[Tuple[str, str]] = [
    ("ab.mean", "ab.mean calculates the average value across a dataset."),
    ("xy.median", "xy.median returns the middle value in a sorted numeric list."),
    ("qp.variance", "qp.variance measures how far values spread from the mean."),
    ("rt.stddev", "rt.stddev computes the standard deviation of numeric data."),
    ("kl.percentile", "kl.percentile retrieves the value at a given percentile rank."),
    ("mv.covariance", "mv.covariance calculates how two variables change together."),
    ("fd.correlation", "fd.correlation computes correlation strength between variables."),
    ("sn.regression", "sn.regression fits a predictive model to input data."),
    ("tj.quantile", "tj.quantile extracts a specific quantile from a dataset."),
    ("uv.autocorrelation", "uv.autocorrelation measures correlation of a signal with itself over time."),
    ("wx.normalization", "wx.normalization rescales numeric data to a standard range."),
    ("ce.smoothing", "ce.smoothing applies a smoothing filter to reduce noise."),
    ("hn.derivative", "hn.derivative estimates the rate of change in a sequence."),
    ("ps.integral", "ps.integral approximates the area under a curve."),
    ("gy.forecast", "gy.forecast predicts future values using historical trends."),
    ("zb.clustering", "zb.clustering groups similar data points into clusters."),
    ("lk.classification", "lk.classification assigns items to predefined categories."),
    ("op.tokenization", "op.tokenization splits text into tokens or meaningful units."),
    ("mq.vectorization", "mq.vectorization converts items into numerical vectors."),
    ("nr.embedding", "nr.embedding produces dense representations of entities."),
    ("vf.anomaly", "vf.anomaly detects outlier or abnormal data patterns."),
    ("du.segmentation", "du.segmentation partitions data into logical segments."),
    ("ke.transformation", "ke.transformation applies linear or nonlinear transformations."),
    ("ri.scaling", "ri.scaling rescales features according to provided factors."),
    ("zo.centering", "zo.centering shifts data so that its mean becomes zero."),
    ("tp.resampling", "tp.resampling generates a new sample distribution."),
    ("qs.interpolation", "qs.interpolation estimates missing values between data points."),
    ("fj.decimation", "fj.decimation reduces sample rate by a fixed factor."),
    ("aw.filtering", "aw.filtering removes unwanted components from signals."),
    ("ic.denoise", "ic.denoise reduces noise using statistical heuristics."),
    ("bd.decomposition", "bd.decomposition breaks a signal into component parts."),
    ("uj.factorization", "uj.factorization decomposes matrices into multiplicative factors."),
    ("eg.rotation", "eg.rotation rotates vectors or matrices in n-dimensional space."),
    ("hy.translation", "hy.translation shifts values by a constant amount."),
    ("cw.windowing", "cw.windowing applies a window function to a signal segment."),
    ("mk.differencing", "mk.differencing computes differences to improve stationarity."),
    ("sl.stationarity", "sl.stationarity evaluates whether a time series is stationary."),
    ("pn.gradient", "pn.gradient computes gradient values across a function."),
    ("ro.hessian", "ro.hessian calculates second-order partial derivatives."),
    ("yl.curvefit", "yl.curvefit fits a custom curve to observed data."),
    ("jx.approximation", "jx.approximation approximates complex functions numerically."),
    ("vd.bias", "vd.bias computes the systematic offset of a model's predictions."),
    ("sf.kurtosis", "sf.kurtosis measures tail extremity of a distribution."),
    ("em.skewness", "em.skewness measures asymmetry of a distribution."),
    ("ag.entropy", "ag.entropy quantifies data unpredictability."),
    ("bh.contrast", "bh.contrast enhances or measures contrast in images or signals."),
    ("ci.sharpen", "ci.sharpen enhances edges or details in data."),
    ("dj.blurring", "dj.blurring reduces detail by convolving with a blur kernel."),
    ("eq.histogram", "eq.histogram computes frequency distribution over value bins."),
    ("fw.binomial", "fw.binomial generates binomial probability distributions."),
    ("gx.poisson", "gx.poisson evaluates Poisson distribution probabilities."),
    ("hv.gaussian", "hv.gaussian produces Gaussian distribution values."),
    ("ij.uniform", "ij.uniform generates uniform distribution samples."),
    ("jk.convergence", "jk.convergence checks whether an algorithm has converged."),
    ("kl.divergence", "kl.divergence computes divergence between two distributions."),
    ("lm.optimization", "lm.optimization finds optimal parameters for a function."),
    ("mn.minimization", "mn.minimization solves for the minimum of a function."),
    ("no.maximization", "no.maximization solves for the maximum of a function."),
    ("op.search", "op.search performs heuristic or structured search operations."),
    ("pq.aggregate", "pq.aggregate combines multiple values using a rule."),
    ("qr.combine", "qr.combine merges datasets or sequences intelligently."),
    ("rs.segment", "rs.segment divides data into logical segments."),
    ("st.cluster", "st.cluster creates groups based on statistical similarity."),
    ("tu.label", "tu.label assigns labels to data based on rules."),
    ("uv.project", "uv.project projects data into a different subspace."),
    ("vw.reduce", "vw.reduce compresses data dimensionality or complexity."),
    ("wx.expand", "wx.expand increases dimensionality with derived features."),
    ("xy.embed", "xy.embed maps items into a lower-dimensional embedding space."),
    ("yz.encode", "yz.encode converts structures into encoded representations."),
    ("za.decode", "za.decode reconstructs data from encoded representations."),
    ("ab.balance", "ab.balance balances class or value distributions."),
    ("bc.weight", "bc.weight applies weights to samples or features."),
    ("cd.score", "cd.score computes evaluation metrics for models."),
    ("de.rank", "de.rank assigns rank ordering to values."),
    ("ef.normalize", "ef.normalize scales values to unit magnitude."),
    ("fg.standardize", "fg.standardize converts data to zero mean and unit variance."),
    ("gh.summarize", "gh.summarize produces statistical summaries of datasets."),
    ("hi.evaluate", "hi.evaluate computes model performance metrics."),
    ("ij.validate", "ij.validate validates a model using test or split data."),
    ("jk.calibrate", "jk.calibrate adjusts a model to improve accuracy."),
    ("kl.measure", "kl.measure calculates specific analytical measurements."),
    ("lm.sample", "lm.sample draws samples from a dataset or distribution."),
    ("mn.bootstrap", "mn.bootstrap generates bootstrap sample sets."),
    ("no.shuffle", "no.shuffle randomly shuffles the ordering of items."),
    ("op.partition", "op.partition splits data into defined partitions."),
    ("pq.align", "pq.align aligns two datasets or sequences for comparison."),
    ("qr.match", "qr.match finds best matches between datasets or patterns."),
    ("rs.extract", "rs.extract pulls relevant features or components from data."),
    ("st.compute", "st.compute performs a general analytical computation."),
    ("tu.simulate", "tu.simulate simulates system behavior using a model."),
    ("uv.model", "uv.model constructs a statistical or predictive model."),
    ("vw.analyze", "vw.analyze analyzes datasets to extract insights."),
    ("wx.optimize", "wx.optimize improves function performance or parameters."),
    ("xy.interpolate", "xy.interpolate fills in missing values using interpolation."),
    ("yz.transform", "yz.transform applies one or more transformations."),
    ("za.integrate", "za.integrate numerically computes an integral over data.")
]
remoted_vertices: List[Tuple[str, str]] = random.sample(vertex_values, k=33)

edge_values: List[Tuple[str, str]] = [
    ("qx.MarketData", "<qx.MarketData instance>"),
    ("lv.ExposureSet", "<lv.ExposureSet instance>"),
    ("dr.RiskFactors", "<dr.RiskFactors instance>"),
    ("pm.ScenarioConfig", "<pm.ScenarioConfig instance>"),
    ("ht.PortfolioState", "<ht.PortfolioState instance>"),
    ("cs.YieldCurve", "<cs.YieldCurve instance>"),
    ("zn.VolSurface", "<zn.VolSurface instance>"),
    ("rf.StressDefinition", "<rf.StressDefinition instance>"),
    ("uk.CreditSpreadData", "<uk.CreditSpreadData instance>"),
    ("bj.LiquidityMetrics", "<bj.LiquidityMetrics instance>"),
    ("wa.TradeSet", "<wa.TradeSet instance>"),
    ("mn.PositionLimits", "<mn.PositionLimits instance>"),
    ("ty.BacktestParameters", "<ty.BacktestParameters instance>"),
    ("so.OptimizationConfig", "<so.OptimizationConfig instance>"),
    ("ae.ForwardCurve", "<ae.ForwardCurve instance>"),
    ("qh.HistoricalPrices", "<qh.HistoricalPrices instance>"),
    ("mv.GreekResults", "<mv.GreekResults instance>"),
    ("xr.PricingContext", "<xr.PricingContext instance>"),
    ("fd.RiskModelConfig", "<fd.RiskModelConfig instance>"),
    ("ul.CalibrationInput", "<ul.CalibrationInput instance>"),
    ("gp.StochasticProcess", "<gp.StochasticProcess instance>"),
    ("nb.FXSpotData", "<nb.FXSpotData instance>"),
    ("td.FundingCurve", "<td.FundingCurve instance>"),
    ("rk.CorrelationMatrix", "<rk.CorrelationMatrix instance>"),
    ("by.MarketRegimes", "<by.MarketRegimes instance>"),
    ("js.AuditTrail", "<js.AuditTrail instance>"),
    ("cl.CreditCurve", "<cl.CreditCurve instance>"),
    ("op.ModelParameters", "<op.ModelParameters instance>"),
    ("ia.StressScenarioSet", "<ia.StressScenarioSet instance>"),
    ("eh.AggregationContext", "<eh.AggregationContext instance>"),
    ("vt.VarResults", "<vt.VarResults instance>"),
    ("wk.SensitivityMap", "<wk.SensitivityMap instance>"),
    ("zf.TradeLifecycleEvent", "<zf.TradeLifecycleEvent instance>"),
    ("du.MarginRequirement", "<du.MarginRequirement instance>"),
    ("sk.PnLSeries", "<sk.PnLSeries instance>"),
    ("bo.TimeSeriesSet", "<bo.TimeSeriesSet instance>"),
    ("pe.PricingKernel", "<pe.PricingKernel instance>"),
    ("yd.AssetUniverse", "<yd.AssetUniverse instance>"),
    ("hg.CollateralSet", "<hg.CollateralSet instance>"),
    ("km.DefaultProbabilities", "<km.DefaultProbabilities instance>"),
    ("rf.MacroFactorData", "<rf.MacroFactorData instance>"),
    ("as.TermStructureSet", "<as.TermStructureSet instance>"),
    ("jj.MarketShockGrid", "<jj.MarketShockGrid instance>"),
    ("cx.StressProjection", "<cx.StressProjection instance>"),
    ("nv.CptyRiskProfile", "<nv.CptyRiskProfile instance>"),
    ("tb.ValuationReport", "<tb.ValuationReport instance>"),
    ("qd.TradeSnapshot", "<qd.TradeSnapshot instance>"),
    ("xi.ModelVersionInfo", "<xi.ModelVersionInfo instance>"),
    ("fm.RiskAttributionSet", "<fm.RiskAttributionSet instance>"),
    ("zl.SimulationConfig", "<zl.SimulationConfig instance>")
]


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
        
        for vertex in vertices:
            # Emit VERTEX_ACCEPTED
            events.append(self.emit_vertex_accepted(graph_key, vertex))
            
            for input_index, (pred_vertex_id, pred_output_index) in enumerate(vertex.inputs):
                # Emit EDGE_ACCEPTED
                events.append(self.emit_edge_accepted(graph_key, vertex.vertex_id, input_index, pred_vertex_id, pred_output_index))
                
                # Emit EDGE_CALCULATED
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
        
        # Emit GRAPH_COMPLETED
        events.append(self.emit_graph_completed(graph_key))
        
        return events

def main():
    generator = MockGraphEventGenerator()
    events = generator.generate_mock_graph_execution(num_vertices=10)
    
    # Print events as JSON
    for event in events:
        print(json.dumps(event))

if __name__ == "__main__":
    main()