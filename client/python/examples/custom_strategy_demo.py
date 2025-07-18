#!/usr/bin/env python3
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
Custom Strategy Demo for AccumulatingTransport

This example demonstrates how to create and use custom aggregation strategies
with the AccumulatingTransport. It shows three different ways to provide
custom strategies:

1. Strategy Instance - Pre-configured instance
2. Strategy Class - Class that gets instantiated
3. Strategy Factory - Function that creates strategy instances
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from openlineage.client.client import Event
from openlineage.client.generated.base import Job, Run
from openlineage.client.run import InputDataset, OutputDataset, RunEvent, RunState
from openlineage.client.transport.accumulating import (
    AccumulatedEvent,
    AccumulatingConfig,
    AccumulatingTransport,
    AggregationStrategyInterface,
)


class JobGroupingStrategy(AggregationStrategyInterface):
    """
    Custom strategy that groups events by job name and aggregates datasets.

    This strategy demonstrates:
    - Custom grouping logic (by job name)
    - Dataset deduplication and merging
    - Configuration-driven behavior
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.group_prefix = config.get("group_prefix", "job-group")
        self.merge_datasets = config.get("merge_datasets", True)
        self.preserve_order = config.get("preserve_order", True)

    def get_group_key(self, event: Event) -> str:
        """Group events by job name."""
        if hasattr(event, "job") and hasattr(event.job, "name"):
            return f"{self.group_prefix}:{event.job.name}"
        return f"{self.group_prefix}:unknown"

    def should_accumulate(self, event: Event) -> bool:
        """Accumulate all RunEvents."""
        return isinstance(event, RunEvent)

    def aggregate_events(self, events: List[AccumulatedEvent]) -> List[Event]:
        """
        Aggregate events by merging datasets and creating summary events.
        """
        if not events:
            return []

        try:
            # Group events by state
            events_by_state = {}
            all_inputs = []
            all_outputs = []

            for acc_event in events:
                event = acc_event.event
                if not isinstance(event, RunEvent):
                    continue

                state = event.eventType
                if state not in events_by_state:
                    events_by_state[state] = []
                events_by_state[state].append(event)

                # Collect all datasets
                if hasattr(event, "inputs") and event.inputs:
                    all_inputs.extend(event.inputs)
                if hasattr(event, "outputs") and event.outputs:
                    all_outputs.extend(event.outputs)

            # Merge datasets if configured
            if self.merge_datasets:
                all_inputs = self._deduplicate_datasets(all_inputs)
                all_outputs = self._deduplicate_datasets(all_outputs)

            # Create aggregated events for each state
            result_events = []

            for state, state_events in events_by_state.items():
                if not state_events:
                    continue

                # Use the latest event as the base
                base_event = state_events[-1] if self.preserve_order else state_events[0]

                # Create enhanced event with aggregated datasets
                aggregated_event = RunEvent(
                    eventType=state,
                    eventTime=base_event.eventTime,
                    run=base_event.run,
                    job=base_event.job,
                    inputs=all_inputs,
                    outputs=all_outputs,
                    producer=f"aggregated-{base_event.producer}",
                )

                result_events.append(aggregated_event)

            return result_events

        except Exception as e:
            print(f"Error in custom aggregation: {e}")
            # Fallback to original events
            return [acc_event.event for acc_event in events]

    def _deduplicate_datasets(self, datasets: List[Any]) -> List[Any]:
        """Remove duplicate datasets based on namespace and name."""
        seen = set()
        unique_datasets = []

        for dataset in datasets:
            if hasattr(dataset, "namespace") and hasattr(dataset, "name"):
                key = f"{dataset.namespace}#{dataset.name}"
                if key not in seen:
                    seen.add(key)
                    unique_datasets.append(dataset)

        return unique_datasets


class MetricsCollectionStrategy(AggregationStrategyInterface):
    """
    Custom strategy that collects metrics about events.

    This strategy demonstrates:
    - Event analysis and metrics collection
    - Custom event generation
    - Stateful aggregation
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.emit_metrics = config.get("emit_metrics", True)
        self.metrics_job_name = config.get("metrics_job_name", "metrics_collection")
        self.metrics = {
            "total_events": 0,
            "events_by_type": {},
            "unique_jobs": set(),
            "unique_runs": set(),
        }

    def get_group_key(self, event: Event) -> str:
        """All events go to the same metrics group."""
        return "metrics_group"

    def should_accumulate(self, event: Event) -> bool:
        """Accumulate all events for metrics."""
        return True

    def aggregate_events(self, events: List[AccumulatedEvent]) -> List[Event]:
        """Generate metrics summary events."""
        if not events:
            return []

        # Analyze events and collect metrics
        self.metrics["total_events"] = len(events)

        for acc_event in events:
            event = acc_event.event

            # Count event types (convert to string for JSON serialization)
            event_type = str(getattr(event, "eventType", "unknown"))
            self.metrics["events_by_type"][event_type] = self.metrics["events_by_type"].get(event_type, 0) + 1

            # Track unique jobs and runs
            if hasattr(event, "job"):
                job_key = f"{event.job.namespace}#{event.job.name}"
                self.metrics["unique_jobs"].add(job_key)

            if hasattr(event, "run"):
                self.metrics["unique_runs"].add(event.run.runId)

        result_events = []

        # Include original events
        result_events.extend([acc_event.event for acc_event in events])

        # Optionally emit metrics event
        if self.emit_metrics:
            metrics_event = self._create_metrics_event()
            result_events.append(metrics_event)

        return result_events

    def _create_metrics_event(self) -> RunEvent:
        """Create a synthetic metrics event."""
        metrics_data = {
            "total_events": self.metrics["total_events"],
            "events_by_type": dict(self.metrics["events_by_type"]),
            "unique_jobs_count": len(self.metrics["unique_jobs"]),
            "unique_runs_count": len(self.metrics["unique_runs"]),
            "collection_time": datetime.now(timezone.utc).isoformat(),
        }

        return RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=str(uuid.uuid4())),
            job=Job(namespace="metrics", name=self.metrics_job_name),
            inputs=[],
            outputs=[
                OutputDataset(
                    namespace="metrics",
                    name="aggregation_metrics",
                    facets={"metrics": {"data": metrics_data}},
                )
            ],
            producer="metrics_collection_strategy",
        )


def create_prioritized_strategy(config: Dict[str, Any]) -> AggregationStrategyInterface:
    """
    Factory function that creates different strategies based on configuration.

    This demonstrates strategy factory pattern for dynamic strategy selection.
    """
    strategy_type = config.get("strategy_type", "job_grouping")

    if strategy_type == "job_grouping":
        return JobGroupingStrategy(config)
    elif strategy_type == "metrics":
        return MetricsCollectionStrategy(config)
    else:
        # Default fallback
        return JobGroupingStrategy(config)


def demo_strategy_instance():
    """Demonstrate using a pre-configured strategy instance."""
    print("=== Demo 1: Custom Strategy Instance ===")

    # Create strategy instance with specific configuration
    strategy = JobGroupingStrategy({"group_prefix": "demo1", "merge_datasets": True, "preserve_order": True})

    config = AccumulatingConfig(
        strategy=strategy,  # Pass instance directly
        emission_policy="count_based",
        emission_config={"max_events": 3, "min_events": 1},
        transport={"type": "console"},
    )

    transport = AccumulatingTransport(config)

    # Emit some test events
    for i in range(3):
        event = RunEvent(
            eventType=RunState.START if i == 0 else RunState.COMPLETE,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=str(uuid.uuid4())),
            job=Job(namespace="demo", name="test_job"),
            inputs=[InputDataset(namespace="input", name=f"data_{i}", facets={})],
            outputs=[OutputDataset(namespace="output", name=f"result_{i}", facets={})],
            producer="demo_system",
        )
        transport.emit(event)

    transport.close()
    print()


def demo_strategy_class():
    """Demonstrate using a strategy class that gets instantiated."""
    print("=== Demo 2: Custom Strategy Class ===")

    config = AccumulatingConfig(
        strategy=MetricsCollectionStrategy,  # Pass class
        strategy_config={"emit_metrics": True, "metrics_job_name": "demo2_metrics"},
        emission_policy="count_based",
        emission_config={"max_events": 2},
        transport={"type": "console"},
    )

    transport = AccumulatingTransport(config)

    # Emit test events
    for i in range(2):
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=str(uuid.uuid4())),
            job=Job(namespace="demo", name=f"job_{i}"),
            inputs=[],
            outputs=[OutputDataset(namespace="demo", name=f"output_{i}", facets={})],
            producer="demo_system",
        )
        transport.emit(event)

    transport.close()
    print()


def demo_strategy_factory():
    """Demonstrate using a factory function to create strategies."""
    print("=== Demo 3: Custom Strategy Factory ===")

    config = AccumulatingConfig(
        strategy=create_prioritized_strategy,  # Pass factory function
        strategy_config={
            "strategy_type": "job_grouping",
            "group_prefix": "factory_demo",
            "merge_datasets": False,
        },
        emission_policy="count_based",
        emission_config={"max_events": 2},
        transport={"type": "console"},
    )

    transport = AccumulatingTransport(config)

    # Emit test events
    events = [
        RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=str(uuid.uuid4())),
            job=Job(namespace="factory", name="process_data"),
            inputs=[InputDataset(namespace="raw", name="input_data", facets={})],
            outputs=[],
            producer="factory_system",
        ),
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=str(uuid.uuid4())),
            job=Job(namespace="factory", name="process_data"),
            inputs=[InputDataset(namespace="raw", name="input_data", facets={})],
            outputs=[OutputDataset(namespace="processed", name="output_data", facets={})],
            producer="factory_system",
        ),
    ]

    for event in events:
        transport.emit(event)

    transport.close()
    print()


def main():
    """Run all custom strategy demonstrations."""
    print("Custom Strategy Demonstrations for AccumulatingTransport")
    print("=" * 60)
    print()

    demo_strategy_instance()
    demo_strategy_class()
    demo_strategy_factory()

    print("=== Summary ===")
    print("Custom strategies provide three ways to extend AccumulatingTransport:")
    print("1. Instance: Pre-configure and pass strategy objects directly")
    print("2. Class: Pass strategy classes that get instantiated with config")
    print("3. Factory: Use functions to dynamically create strategies")
    print()
    print("This flexibility allows you to:")
    print("- Implement domain-specific aggregation logic")
    print("- Create reusable strategy components")
    print("- Support dynamic strategy selection")
    print("- Integrate with existing systems and patterns")


if __name__ == "__main__":
    main()
