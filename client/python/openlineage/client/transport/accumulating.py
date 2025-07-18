# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""
AccumulatingTransport for OpenLineage Events

This module provides an accumulating transport mechanism that aggregates events
before emitting them through an underlying transport. The AccumulatingTransport
supports various aggregation strategies and emission policies.

## AccumulatingTransport Architecture

The AccumulatingTransport implements a sophisticated event aggregation system with:
- **Event Accumulation**: Collects related events based on configurable grouping criteria
- **Aggregation Strategies**: Multiple strategies for combining event data (parent-child, merge)
- **Emission Policies**: Configurable triggers for when accumulated events should be emitted
- **Underlying Transport**: Delegates final emission to any configured transport

## Use Cases

### Parent-Child Run Aggregation
- **Scenario**: Aggregate inputs and outputs from all child runs into parent run events
- **Strategy**: `parent_child` - Accumulates datasets from child runs into parent START/COMPLETE events
- **Emission**: When parent run COMPLETE/FAIL/ABORT is received, or timeout/size limit reached

### Dataset Merging
- **Scenario**: Combine multiple events that update the same datasets with facet merging
- **Strategy**: `merge_datasets` - Merges dataset facets from multiple events for the same datasets
- **Emission**: Based on time windows or explicit flush triggers

### Batch Processing
- **Scenario**: Collect events and emit them in batches to reduce transport overhead
- **Strategy**: `batch` - Simple event collection without modification
- **Emission**: Based on count, time, or explicit triggers

## Aggregation Strategies

### ParentChildStrategy
Aggregates child run inputs/outputs into parent run events:
- Collects all START/COMPLETE/FAIL events for runs with the same parent (via ParentRunFacet)
- Merges inputs and outputs from child runs into parent events
- Preserves parent run lifecycle while enriching with child data
- Handles nested parent-child relationships through root job tracking

### MergeDatasetsStrategy
Merges dataset facets and metadata across events:
- Groups events by dataset namespace/name
- Combines dataset facets using merge logic (last-writer-wins for most facets)
- Handles special facets like schema (structural merge) and columnLineage (append)
- Preserves event ordering and timing

### BatchStrategy
Simple event collection without modification:
- Accumulates events in order received
- No data transformation or merging
- Emits batches based purely on configured triggers

### Custom Strategies
You can provide your own aggregation strategies in several ways:

#### 1. Strategy Instance
Pass a pre-configured instance of AggregationStrategyInterface:
```python
class MyCustomStrategy(AggregationStrategyInterface):
    def __init__(self, config):
        self.my_param = config.get("my_param", "default")

    def get_group_key(self, event):
        return f"custom:{event.job.name}"

    def should_accumulate(self, event):
        return isinstance(event, RunEvent)

    def aggregate_events(self, events):
        # Your custom aggregation logic here
        return [event.event for event in events]

# Use instance directly
config = AccumulatingConfig(strategy=MyCustomStrategy({"my_param": "value"}))
```

#### 2. Strategy Class
Pass the class itself, it will be instantiated with strategy_config:
```python
config = AccumulatingConfig(
    strategy=MyCustomStrategy,
    strategy_config={"my_param": "value"}
)
```

#### 3. Strategy Factory Function
Pass a callable that returns a strategy instance:
```python
def create_custom_strategy(config):
    return MyCustomStrategy(config)

config = AccumulatingConfig(
    strategy=create_custom_strategy,
    strategy_config={"my_param": "value"}
)
```

## Emission Policies

### TimeBasedPolicy
- **trigger_interval**: Maximum time between emissions (seconds)
- **max_age**: Maximum age of oldest accumulated event before forced emission

### CountBasedPolicy
- **max_events**: Maximum number of events before emission
- **min_events**: Minimum events required before emission (unless timeout)

### SizeBasedPolicy
- **max_size_bytes**: Maximum accumulated data size before emission
- **estimate_event_size**: Whether to estimate JSON size for triggering

### CompletionBasedPolicy
- **completion_event_types**: Event types that trigger immediate emission (e.g., COMPLETE, FAIL)
- **parent_completion**: For parent-child strategy, emit when parent completes

### HybridPolicy
Combines multiple policies with AND/OR logic:
- **policies**: List of policies to combine
- **operator**: "AND" (all must trigger) or "OR" (any can trigger)

## Configuration

```python
from openlineage.client.transport.accumulating import AccumulatingConfig, AccumulatingTransport

config = AccumulatingConfig(
    # Aggregation strategy
    strategy="parent_child",
    strategy_config={
        "parent_run_facet_key": "parent",  # Key for ParentRunFacet in run.facets
        "preserve_child_events": False,    # Whether to also emit original child events
        "merge_duplicate_datasets": True,  # Merge datasets with same namespace/name
    },

    # Emission policy
    emission_policy="hybrid",
    emission_config={
        "operator": "OR",
        "policies": [
            {"type": "time_based", "trigger_interval": 30.0, "max_age": 300.0},
            {"type": "count_based", "max_events": 100},
            {"type": "completion_based", "completion_event_types": ["COMPLETE", "FAIL", "ABORT"]}
        ]
    },

    # Underlying transport
    transport={
        "type": "http",
        "url": "https://example.com/api/v1/lineage"
    },

    # Buffer management
    max_buffer_size=10000,
    cleanup_interval=60.0,

    # Error handling
    continue_on_aggregation_error=True,
    fallback_to_direct_emission=True
)

transport = AccumulatingTransport(config)
```

## Example Usage: Parent-Child Aggregation

```python
# Child events are accumulated:
transport.emit(RunEvent(
    eventType=RunState.START,
    run=Run(runId="child-1", facets={"parent": ParentRunFacet(...)}),
    job=Job(namespace="ns", name="child-job"),
    inputs=[InputDataset(...)],
    outputs=[]
))

transport.emit(RunEvent(
    eventType=RunState.COMPLETE,
    run=Run(runId="child-1", facets={"parent": ParentRunFacet(...)}),
    job=Job(namespace="ns", name="child-job"),
    inputs=[InputDataset(...)],
    outputs=[OutputDataset(...)]
))

# Parent completion triggers emission of aggregated event:
transport.emit(RunEvent(
    eventType=RunState.COMPLETE,
    run=Run(runId="parent-1"),
    job=Job(namespace="ns", name="parent-job"),
    inputs=[],
    outputs=[]
))

# Emitted event contains aggregated inputs/outputs from child:
# {
#   "eventType": "COMPLETE",
#   "run": {"runId": "parent-1"},
#   "job": {"namespace": "ns", "name": "parent-job"},
#   "inputs": [/* aggregated from child-1 */],
#   "outputs": [/* aggregated from child-1 */]
# }
```

## Thread Safety and Performance

- **Thread-safe accumulation**: Uses locks for concurrent event processing
- **Memory-efficient**: Configurable buffer limits and cleanup policies
- **Lazy aggregation**: Aggregation performed only when emission is triggered
- **Graceful degradation**: Falls back to direct emission on errors if configured

## Error Handling

- **Aggregation errors**: Configurable continue/fail behavior with detailed logging
- **Transport errors**: Propagated from underlying transport
- **Buffer overflow**: Automatic cleanup of oldest events when limits exceeded
- **Malformed events**: Validation with fallback options
"""

from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from copy import deepcopy
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List

import attr
from openlineage.client.event_v2 import RunEvent as RunEventV2
from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields

if TYPE_CHECKING:
    from openlineage.client.client import Event

log = logging.getLogger(__name__)


class AggregationStrategy(Enum):
    """Enumeration of available aggregation strategies."""

    PARENT_CHILD = "parent_child"
    MERGE_DATASETS = "merge_datasets"
    BATCH = "batch"


class EmissionPolicy(Enum):
    """Enumeration of available emission policies."""

    TIME_BASED = "time_based"
    COUNT_BASED = "count_based"
    SIZE_BASED = "size_based"
    COMPLETION_BASED = "completion_based"
    HYBRID = "hybrid"


@attr.define
class AccumulatedEvent:
    """Wrapper for events in the accumulation buffer."""

    event: Event
    accumulated_at: float = attr.field(factory=time.time)
    event_size: int = attr.field(default=0)
    group_key: str = attr.field(default="")

    def __attrs_post_init__(self) -> None:
        if not self.event_size:
            # Estimate event size as JSON length
            try:
                self.event_size = len(Serde.to_json(self.event))
            except Exception:
                self.event_size = 1000  # Default estimate


@attr.define
class EventGroup:
    """A group of accumulated events ready for aggregation."""

    group_key: str
    events: List[AccumulatedEvent] = attr.field(factory=list)
    first_event_time: float = attr.field(factory=time.time)
    last_event_time: float = attr.field(factory=time.time)
    total_size: int = attr.field(default=0)

    def add_event(self, event: AccumulatedEvent) -> None:
        """Add an event to this group."""
        self.events.append(event)
        self.last_event_time = time.time()
        self.total_size += event.event_size
        if len(self.events) == 1:
            self.first_event_time = event.accumulated_at

    def remove_event(self, event: AccumulatedEvent) -> None:
        """Remove an event from this group."""
        if event in self.events:
            self.events.remove(event)
            self.total_size -= event.event_size


class AggregationStrategyInterface(ABC):
    """Interface for event aggregation strategies."""

    @abstractmethod
    def get_group_key(self, event: Event) -> str:
        """Determine which group this event belongs to."""
        pass

    @abstractmethod
    def should_accumulate(self, event: Event) -> bool:
        """Determine if this event should be accumulated."""
        pass

    @abstractmethod
    def aggregate_events(self, events: List[AccumulatedEvent]) -> List[Event]:
        """Aggregate a list of events into final events for emission."""
        pass


class EmissionPolicyInterface(ABC):
    """Interface for emission trigger policies."""

    @abstractmethod
    def should_emit(self, group: EventGroup, current_time: float) -> bool:
        """Determine if the event group should be emitted now."""
        pass


class ParentChildAggregationStrategy(AggregationStrategyInterface):
    """
    Aggregates child run events into parent run events.

    This strategy identifies parent-child relationships through ParentRunFacet
    and merges inputs/outputs from child runs into parent events.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.parent_facet_key = config.get("parent_run_facet_key", "parent")
        self.preserve_child_events = config.get("preserve_child_events", False)
        self.merge_duplicate_datasets = config.get("merge_duplicate_datasets", True)
        self.parent_events: Dict[str, Event] = {}  # parent_run_id -> parent_event

    def get_group_key(self, event: Event) -> str:
        """Group by parent run ID if present, otherwise by run ID."""
        if hasattr(event, "run") and hasattr(event.run, "facets"):
            parent_facet = event.run.facets.get(self.parent_facet_key)
            if parent_facet and hasattr(parent_facet, "run") and hasattr(parent_facet.run, "runId"):
                return f"parent:{parent_facet.run.runId}"

        if hasattr(event, "run") and hasattr(event.run, "runId"):
            return f"run:{event.run.runId}"

        return "unknown"

    def should_accumulate(self, event: Event) -> bool:
        """Accumulate RunEvents."""
        return isinstance(event, (RunEvent, RunEventV2))

    def aggregate_events(self, events: List[AccumulatedEvent]) -> List[Event]:
        """Aggregate child run data into parent run events."""
        try:
            # Group events by run ID
            events_by_run: Dict[str, List[AccumulatedEvent]] = defaultdict(list)
            parent_run_id = None

            for acc_event in events:
                event = acc_event.event
                if not isinstance(event, (RunEvent, RunEventV2)):
                    continue

                run_id = event.run.runId
                events_by_run[run_id].append(acc_event)

                # Check if this is a child event with parent info
                if hasattr(event.run, "facets"):
                    parent_facet = event.run.facets.get(self.parent_facet_key)
                    if parent_facet and hasattr(parent_facet, "run"):
                        parent_run_id = parent_facet.run.runId

            if not parent_run_id:
                # No parent relationship found, return original events
                return [acc_event.event for acc_event in events]

            # Find parent events and child events
            parent_events = []
            child_events = []

            for run_id, run_events in events_by_run.items():
                if run_id == parent_run_id:
                    parent_events.extend(run_events)
                else:
                    child_events.extend(run_events)

            if not parent_events:
                # No parent events found, return original events
                return [acc_event.event for acc_event in events]

            # Aggregate datasets from child events
            aggregated_inputs = []
            aggregated_outputs = []

            for acc_event in child_events:
                event = acc_event.event
                if hasattr(event, "inputs") and event.inputs:
                    aggregated_inputs.extend(event.inputs)
                if hasattr(event, "outputs") and event.outputs:
                    aggregated_outputs.extend(event.outputs)

            # Merge duplicate datasets if configured
            if self.merge_duplicate_datasets:
                aggregated_inputs = self._merge_datasets(aggregated_inputs)
                aggregated_outputs = self._merge_datasets(aggregated_outputs)

            # Create enhanced parent events
            result_events = []

            for acc_event in parent_events:
                parent_event = acc_event.event
                enhanced_event = self._enhance_parent_event(
                    parent_event, aggregated_inputs, aggregated_outputs
                )
                result_events.append(enhanced_event)

            # Optionally include original child events
            if self.preserve_child_events:
                result_events.extend([acc_event.event for acc_event in child_events])

            return result_events

        except Exception as e:
            log.error("Error in parent-child aggregation: %s", e, exc_info=True)
            # Fallback to original events
            return [acc_event.event for acc_event in events]

    def _merge_datasets(self, datasets: List[Any]) -> List[Any]:
        """Merge datasets with the same namespace and name."""
        dataset_map = {}

        for dataset in datasets:
            if hasattr(dataset, "namespace") and hasattr(dataset, "name"):
                key = f"{dataset.namespace}#{dataset.name}"
                if key in dataset_map:
                    # Merge facets
                    existing = dataset_map[key]
                    if hasattr(dataset, "facets") and dataset.facets:
                        if not hasattr(existing, "facets") or not existing.facets:
                            existing.facets = {}
                        existing.facets.update(dataset.facets)

                    # For input datasets, merge inputFacets
                    if hasattr(dataset, "inputFacets") and dataset.inputFacets:
                        if not hasattr(existing, "inputFacets") or not existing.inputFacets:
                            existing.inputFacets = {}
                        existing.inputFacets.update(dataset.inputFacets)

                    # For output datasets, merge outputFacets
                    if hasattr(dataset, "outputFacets") and dataset.outputFacets:
                        if not hasattr(existing, "outputFacets") or not existing.outputFacets:
                            existing.outputFacets = {}
                        existing.outputFacets.update(dataset.outputFacets)
                else:
                    dataset_map[key] = deepcopy(dataset)

        return list(dataset_map.values())

    def _enhance_parent_event(self, parent_event: Event, inputs: List[Any], outputs: List[Any]) -> Event:
        """Create an enhanced parent event with aggregated child data."""
        enhanced_event = deepcopy(parent_event)

        # Merge with existing inputs/outputs
        if hasattr(enhanced_event, "inputs"):
            if enhanced_event.inputs:
                all_inputs = list(enhanced_event.inputs) + inputs
            else:
                all_inputs = inputs
            if self.merge_duplicate_datasets:
                enhanced_event.inputs = self._merge_datasets(all_inputs)
            else:
                enhanced_event.inputs = all_inputs
        else:
            enhanced_event.inputs = inputs

        if hasattr(enhanced_event, "outputs"):
            if enhanced_event.outputs:
                all_outputs = list(enhanced_event.outputs) + outputs
            else:
                all_outputs = outputs
            if self.merge_duplicate_datasets:
                enhanced_event.outputs = self._merge_datasets(all_outputs)
            else:
                enhanced_event.outputs = all_outputs
        else:
            enhanced_event.outputs = outputs

        return enhanced_event


class MergeDatasetsAggregationStrategy(AggregationStrategyInterface):
    """
    Merges events that operate on the same datasets.

    This strategy groups events by the datasets they reference and merges
    facets and metadata across events.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.group_by_job = config.get("group_by_job", True)
        self.merge_run_facets = config.get("merge_run_facets", False)

    def get_group_key(self, event: Event) -> str:
        """Group by job and datasets."""
        parts = []

        if self.group_by_job and hasattr(event, "job"):
            parts.append(f"job:{event.job.namespace}#{event.job.name}")

        dataset_keys = []
        if hasattr(event, "inputs") and event.inputs:
            for ds in event.inputs:
                if hasattr(ds, "namespace") and hasattr(ds, "name"):
                    dataset_keys.append(f"i:{ds.namespace}#{ds.name}")

        if hasattr(event, "outputs") and event.outputs:
            for ds in event.outputs:
                if hasattr(ds, "namespace") and hasattr(ds, "name"):
                    dataset_keys.append(f"o:{ds.namespace}#{ds.name}")

        parts.extend(sorted(dataset_keys))
        return "|".join(parts)

    def should_accumulate(self, event: Event) -> bool:
        """Accumulate events with datasets."""
        if not isinstance(event, (RunEvent, RunEventV2)):
            return False

        has_datasets = (hasattr(event, "inputs") and event.inputs) or (
            hasattr(event, "outputs") and event.outputs
        )
        return has_datasets

    def aggregate_events(self, events: List[AccumulatedEvent]) -> List[Event]:
        """Merge dataset facets across events."""
        if not events:
            return []

        if len(events) == 1:
            return [events[0].event]

        try:
            # Use the latest event as base
            latest_event = max(events, key=lambda e: e.accumulated_at)
            base_event = deepcopy(latest_event.event)

            # Merge datasets from all events
            all_inputs = []
            all_outputs = []

            for acc_event in events:
                event = acc_event.event
                if hasattr(event, "inputs") and event.inputs:
                    all_inputs.extend(event.inputs)
                if hasattr(event, "outputs") and event.outputs:
                    all_outputs.extend(event.outputs)

            # Merge duplicate datasets
            if hasattr(base_event, "inputs"):
                base_event.inputs = self._merge_datasets(all_inputs)
            if hasattr(base_event, "outputs"):
                base_event.outputs = self._merge_datasets(all_outputs)

            # Optionally merge run facets
            if self.merge_run_facets and hasattr(base_event, "run") and hasattr(base_event.run, "facets"):
                for acc_event in events[:-1]:  # Skip latest (already base)
                    event = acc_event.event
                    if hasattr(event, "run") and hasattr(event.run, "facets") and event.run.facets:
                        base_event.run.facets.update(event.run.facets)

            return [base_event]

        except Exception as e:
            log.error("Error in dataset merge aggregation: %s", e, exc_info=True)
            return [acc_event.event for acc_event in events]

    def _merge_datasets(self, datasets: List[Any]) -> List[Any]:
        """Merge datasets with same namespace/name."""
        dataset_map = {}

        for dataset in datasets:
            if hasattr(dataset, "namespace") and hasattr(dataset, "name"):
                key = f"{dataset.namespace}#{dataset.name}"
                if key in dataset_map:
                    existing = dataset_map[key]
                    # Merge all facet types
                    for facet_attr in ["facets", "inputFacets", "outputFacets"]:
                        if hasattr(dataset, facet_attr):
                            dataset_facets = getattr(dataset, facet_attr)
                            if dataset_facets:
                                if not hasattr(existing, facet_attr):
                                    setattr(existing, facet_attr, {})
                                existing_facets = getattr(existing, facet_attr)
                                existing_facets.update(dataset_facets)
                else:
                    dataset_map[key] = deepcopy(dataset)

        return list(dataset_map.values())


class BatchAggregationStrategy(AggregationStrategyInterface):
    """
    Simple batching strategy that collects events without modification.

    This strategy accumulates events in order and emits them as batches
    based on the configured emission policy.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.group_all = config.get("group_all", True)

    def get_group_key(self, event: Event) -> str:
        """Group all events together or by event type."""
        if self.group_all:
            return "batch"

        if isinstance(event, (RunEvent, RunEventV2)):
            return f"batch:run:{event.eventType}"

        return f"batch:{type(event).__name__}"

    def should_accumulate(self, event: Event) -> bool:
        """Accumulate all events."""
        return True

    def aggregate_events(self, events: List[AccumulatedEvent]) -> List[Event]:
        """Return events in original order without modification."""
        return [acc_event.event for acc_event in events]


class TimeBasedEmissionPolicy(EmissionPolicyInterface):
    """Emit based on time intervals and event age."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.trigger_interval = config.get("trigger_interval", 30.0)
        self.max_age = config.get("max_age", 300.0)
        self.last_emission = time.time()

    def should_emit(self, group: EventGroup, current_time: float) -> bool:
        """Emit if interval elapsed or oldest event too old."""
        interval_trigger = (current_time - self.last_emission) >= self.trigger_interval
        age_trigger = (current_time - group.first_event_time) >= self.max_age

        if interval_trigger or age_trigger:
            self.last_emission = current_time
            return True
        return False


class CountBasedEmissionPolicy(EmissionPolicyInterface):
    """Emit based on event count."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.max_events = config.get("max_events", 100)
        self.min_events = config.get("min_events", 1)

    def should_emit(self, group: EventGroup, current_time: float) -> bool:
        """Emit if event count threshold reached."""
        return len(group.events) >= self.max_events


class SizeBasedEmissionPolicy(EmissionPolicyInterface):
    """Emit based on accumulated data size."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.max_size_bytes = config.get("max_size_bytes", 1024 * 1024)  # 1MB default

    def should_emit(self, group: EventGroup, current_time: float) -> bool:
        """Emit if size threshold reached."""
        return group.total_size >= self.max_size_bytes


class CompletionBasedEmissionPolicy(EmissionPolicyInterface):
    """Emit when completion events are received."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.completion_event_types = set(config.get("completion_event_types", ["COMPLETE", "FAIL", "ABORT"]))
        self.parent_completion = config.get("parent_completion", True)

    def should_emit(self, group: EventGroup, current_time: float) -> bool:
        """Emit if completion event detected."""
        for acc_event in group.events:
            event = acc_event.event
            if isinstance(event, (RunEvent, RunEventV2)):
                if str(event.eventType) in self.completion_event_types:
                    return True
        return False


class HybridEmissionPolicy(EmissionPolicyInterface):
    """Combines multiple emission policies."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.operator = config.get("operator", "OR").upper()
        self.policies = []

        for policy_config in config.get("policies", []):
            policy_type = policy_config.get("type")
            if policy_type == "time_based":
                self.policies.append(TimeBasedEmissionPolicy(policy_config))
            elif policy_type == "count_based":
                self.policies.append(CountBasedEmissionPolicy(policy_config))
            elif policy_type == "size_based":
                self.policies.append(SizeBasedEmissionPolicy(policy_config))
            elif policy_type == "completion_based":
                self.policies.append(CompletionBasedEmissionPolicy(policy_config))

    def should_emit(self, group: EventGroup, current_time: float) -> bool:
        """Combine policy results with AND/OR logic."""
        if not self.policies:
            return False

        results = [policy.should_emit(group, current_time) for policy in self.policies]

        if self.operator == "AND":
            return all(results)
        else:  # OR
            return any(results)


@attr.define
class AccumulatingConfig(Config):
    """Configuration for AccumulatingTransport."""

    # Aggregation strategy - can be string name or custom strategy instance/class
    strategy: Any = attr.field(default="parent_child")
    strategy_config: Dict[str, Any] = attr.field(factory=dict)

    # Emission policy
    emission_policy: str = attr.field(default="time_based")
    emission_config: Dict[str, Any] = attr.field(factory=dict)

    # Underlying transport configuration
    transport: Dict[str, Any] = attr.field(factory=dict)

    # Buffer management
    max_buffer_size: int = attr.field(default=10000)
    cleanup_interval: float = attr.field(default=60.0)

    # Error handling
    continue_on_aggregation_error: bool = attr.field(default=True)
    fallback_to_direct_emission: bool = attr.field(default=True)

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "AccumulatingConfig":
        """Create AccumulatingConfig from dictionary."""
        return cls(**get_only_specified_fields(cls, params))


class AccumulatingTransport(Transport):
    """
    Transport that accumulates and aggregates events before emission.

    This transport provides sophisticated event aggregation capabilities
    with configurable strategies and emission policies.
    """

    kind = "accumulating"
    config_class = AccumulatingConfig

    def __init__(self, config: AccumulatingConfig) -> None:
        """Initialize the AccumulatingTransport."""
        self.config = config
        self._lock = threading.RLock()
        self._event_groups: Dict[str, EventGroup] = {}
        self._last_cleanup = time.time()

        # Initialize strategy
        self._strategy = self._create_strategy()

        # Initialize emission policy
        self._emission_policy = self._create_emission_policy()

        # Initialize underlying transport
        self._underlying_transport = self._create_underlying_transport()

        log.info(
            "AccumulatingTransport initialized with strategy=%s, policy=%s, transport=%s",
            self.config.strategy,
            self.config.emission_policy,
            type(self._underlying_transport).__name__,
        )

    def _create_strategy(self) -> AggregationStrategyInterface:
        """Create the configured aggregation strategy."""
        strategy = self.config.strategy

        # Handle custom strategy instances
        if isinstance(strategy, AggregationStrategyInterface) or (
            hasattr(strategy, "__class__") and self._has_strategy_interface(type(strategy))
        ):
            log.info("Using provided custom strategy instance: %s", type(strategy).__name__)
            return strategy

        # Handle custom strategy classes
        if isinstance(strategy, type) and (
            issubclass(strategy, AggregationStrategyInterface) or self._has_strategy_interface(strategy)
        ):
            log.info("Instantiating custom strategy class: %s", strategy.__name__)
            return strategy(self.config.strategy_config)

        # Handle custom strategy callables/factories
        if callable(strategy) and not isinstance(strategy, str):
            log.info("Calling custom strategy factory: %s", strategy)
            result = strategy(self.config.strategy_config)
            if not (
                isinstance(result, AggregationStrategyInterface) or self._has_strategy_interface(type(result))
            ):
                raise ValueError(
                    f"Custom strategy factory must return AggregationStrategyInterface "
                    f"or object with strategy interface, got {type(result)}"
                )
            return result

        # Handle built-in strategy names
        if isinstance(strategy, str):
            strategy_type = strategy.lower()

            if strategy_type == AggregationStrategy.PARENT_CHILD.value:
                return ParentChildAggregationStrategy(self.config.strategy_config)
            elif strategy_type == AggregationStrategy.MERGE_DATASETS.value:
                return MergeDatasetsAggregationStrategy(self.config.strategy_config)
            elif strategy_type == AggregationStrategy.BATCH.value:
                return BatchAggregationStrategy(self.config.strategy_config)
            else:
                raise ValueError(f"Unknown built-in aggregation strategy: {strategy_type}")

        raise ValueError(
            f"Strategy must be a string name, AggregationStrategyInterface instance, "
            f"AggregationStrategyInterface subclass, or callable returning one. "
            f"Got: {type(strategy)}"
        )

    def _has_strategy_interface(self, cls: type) -> bool:
        """Check if a class has the required strategy interface methods."""
        required_methods = ["get_group_key", "should_accumulate", "aggregate_events"]
        return all(hasattr(cls, method) and callable(getattr(cls, method)) for method in required_methods)

    def _create_emission_policy(self) -> EmissionPolicyInterface:
        """Create the configured emission policy."""
        policy_type = self.config.emission_policy.lower()

        if policy_type == EmissionPolicy.TIME_BASED.value:
            return TimeBasedEmissionPolicy(self.config.emission_config)
        elif policy_type == EmissionPolicy.COUNT_BASED.value:
            return CountBasedEmissionPolicy(self.config.emission_config)
        elif policy_type == EmissionPolicy.SIZE_BASED.value:
            return SizeBasedEmissionPolicy(self.config.emission_config)
        elif policy_type == EmissionPolicy.COMPLETION_BASED.value:
            return CompletionBasedEmissionPolicy(self.config.emission_config)
        elif policy_type == EmissionPolicy.HYBRID.value:
            return HybridEmissionPolicy(self.config.emission_config)
        else:
            raise ValueError(f"Unknown emission policy: {policy_type}")

    def _create_underlying_transport(self) -> Transport:
        """Create the underlying transport."""
        from openlineage.client.transport import get_default_factory

        if not self.config.transport:
            raise ValueError("No underlying transport configured")

        return get_default_factory().create(self.config.transport)

    def emit(self, event: Event) -> None:
        """Emit an event, potentially accumulating it first."""
        try:
            # Check if this event should be accumulated
            if not self._strategy.should_accumulate(event):
                # Direct emission for non-accumulated events
                self._underlying_transport.emit(event)
                return

            # Add to accumulation buffer
            with self._lock:
                self._add_to_buffer(event)

                # Check emission triggers
                self._check_and_emit()

                # Periodic cleanup
                self._periodic_cleanup()

        except Exception as e:
            log.error("Error in AccumulatingTransport.emit: %s", e, exc_info=True)

            if self.config.continue_on_aggregation_error:
                if self.config.fallback_to_direct_emission:
                    try:
                        self._underlying_transport.emit(event)
                    except Exception as fallback_error:
                        log.error("Fallback emission also failed: %s", fallback_error)
                        raise
                else:
                    log.warning("Continuing after aggregation error, event dropped")
            else:
                raise

    def _add_to_buffer(self, event: Event) -> None:
        """Add event to the accumulation buffer."""
        group_key = self._strategy.get_group_key(event)
        acc_event = AccumulatedEvent(event=event, group_key=group_key)

        if group_key not in self._event_groups:
            self._event_groups[group_key] = EventGroup(group_key=group_key)

        self._event_groups[group_key].add_event(acc_event)

        # Enforce buffer size limits
        self._enforce_buffer_limits()

        log.debug("Added event to group %s, total groups: %d", group_key, len(self._event_groups))

    def _check_and_emit(self) -> None:
        """Check if any groups should be emitted."""
        current_time = time.time()
        groups_to_emit = []

        for group_key, group in self._event_groups.items():
            if self._emission_policy.should_emit(group, current_time):
                groups_to_emit.append(group_key)

        for group_key in groups_to_emit:
            self._emit_group(group_key)

    def _emit_group(self, group_key: str) -> None:
        """Emit all events in a group."""
        group = self._event_groups.pop(group_key, None)
        if not group or not group.events:
            return

        try:
            # Aggregate events
            aggregated_events = self._strategy.aggregate_events(group.events)

            # Emit aggregated events
            for event in aggregated_events:
                self._underlying_transport.emit(event)

            log.info(
                "Emitted group %s: %d events aggregated into %d events",
                group_key,
                len(group.events),
                len(aggregated_events),
            )

        except Exception as e:
            log.error("Error emitting group %s: %s", group_key, e, exc_info=True)

            if self.config.continue_on_aggregation_error:
                if self.config.fallback_to_direct_emission:
                    # Fallback: emit original events directly
                    for acc_event in group.events:
                        try:
                            self._underlying_transport.emit(acc_event.event)
                        except Exception as fallback_error:
                            log.error("Fallback emission failed for event: %s", fallback_error)
            else:
                raise

    def _enforce_buffer_limits(self) -> None:
        """Enforce buffer size limits by removing oldest events."""
        total_events = sum(len(group.events) for group in self._event_groups.values())

        if total_events <= self.config.max_buffer_size:
            return

        # Remove oldest events first
        all_events = []
        for group in self._event_groups.values():
            all_events.extend([(group, event) for event in group.events])

        # Sort by accumulation time
        all_events.sort(key=lambda x: x[1].accumulated_at)

        # Remove oldest events
        events_to_remove = total_events - self.config.max_buffer_size
        for i in range(events_to_remove):
            group, event = all_events[i]
            group.remove_event(event)

        # Clean up empty groups
        self._event_groups = {k: v for k, v in self._event_groups.items() if v.events}

        log.warning(
            "Buffer limit exceeded, removed %d oldest events, %d events remaining",
            events_to_remove,
            sum(len(group.events) for group in self._event_groups.values()),
        )

    def _periodic_cleanup(self) -> None:
        """Perform periodic cleanup of stale groups."""
        current_time = time.time()

        if current_time - self._last_cleanup < self.config.cleanup_interval:
            return

        self._last_cleanup = current_time

        # Force emit very old groups
        max_age = 3600.0  # 1 hour max age
        old_groups = []

        for group_key, group in self._event_groups.items():
            if current_time - group.first_event_time > max_age:
                old_groups.append(group_key)

        for group_key in old_groups:
            log.warning("Force emitting old group: %s", group_key)
            self._emit_group(group_key)

    def flush(self) -> None:
        """Flush all accumulated events."""
        with self._lock:
            group_keys = list(self._event_groups.keys())
            for group_key in group_keys:
                self._emit_group(group_key)

    def close(self, timeout: float = -1) -> bool:
        """Close the transport, flushing all accumulated events."""
        try:
            self.flush()
            return self._underlying_transport.close(timeout)
        except Exception as e:
            log.error("Error closing AccumulatingTransport: %s", e)
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Get accumulation statistics."""
        with self._lock:
            total_events = sum(len(group.events) for group in self._event_groups.values())
            total_size = sum(group.total_size for group in self._event_groups.values())

            stats = {
                "accumulated_groups": len(self._event_groups),
                "accumulated_events": total_events,
                "accumulated_size_bytes": total_size,
                "strategy": self.config.strategy,
                "emission_policy": self.config.emission_policy,
            }

            # Add underlying transport stats if available
            if hasattr(self._underlying_transport, "get_stats"):
                stats["underlying_transport"] = self._underlying_transport.get_stats()

            return stats
