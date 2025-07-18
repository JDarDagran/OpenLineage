#!/usr/bin/env python3
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
Quick Demo: Custom Strategy with Visible Output

This shows a simple custom strategy in action with console output
to demonstrate how custom strategies work.
"""

import uuid
from datetime import datetime, timezone

from openlineage.client.generated.base import Job, Run
from openlineage.client.run import InputDataset, OutputDataset, RunEvent, RunState
from openlineage.client.transport.accumulating import AccumulatingConfig, AccumulatingTransport


class PrefixingStrategy:
    """Custom strategy that adds prefixes to job names in aggregated events."""

    def __init__(self, config):
        self.prefix = config.get("prefix", "CUSTOM")

    def get_group_key(self, event):
        return f"group:{self.prefix}"

    def should_accumulate(self, event):
        return isinstance(event, RunEvent)

    def aggregate_events(self, events):
        """Modify events to add prefix to job names."""
        result = []
        for acc_event in events:
            event = acc_event.event

            # Create modified event with prefixed job name
            modified_event = RunEvent(
                eventType=event.eventType,
                eventTime=event.eventTime,
                run=event.run,
                job=Job(namespace=event.job.namespace, name=f"{self.prefix}_{event.job.name}"),
                inputs=event.inputs if hasattr(event, "inputs") else [],
                outputs=event.outputs if hasattr(event, "outputs") else [],
                producer=f"custom_{event.producer}",
            )
            result.append(modified_event)

        return result


def main():
    print("=== Custom Strategy Demo with Visible Output ===")

    # Configure transport with custom strategy
    config = AccumulatingConfig(
        strategy=PrefixingStrategy,
        strategy_config={"prefix": "DEMO"},
        emission_policy="count_based",
        emission_config={"max_events": 2},
        transport={"type": "console"},
    )

    transport = AccumulatingTransport(config)

    print("\nEmitting events (will accumulate until count=2):")

    # Emit first event
    print("1. Emitting START event...")
    event1 = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=str(uuid.uuid4())),
        job=Job(namespace="demo", name="process_data"),
        inputs=[InputDataset(namespace="raw", name="input.csv", facets={})],
        outputs=[],
        producer="demo_system",
    )
    transport.emit(event1)

    # Emit second event (will trigger emission)
    print("2. Emitting COMPLETE event (triggers aggregation)...")
    event2 = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=str(uuid.uuid4())),
        job=Job(namespace="demo", name="process_data"),
        inputs=[InputDataset(namespace="raw", name="input.csv", facets={})],
        outputs=[OutputDataset(namespace="processed", name="output.csv", facets={})],
        producer="demo_system",
    )
    transport.emit(event2)

    print("\n👆 Notice how the job names now have 'DEMO_' prefix!")
    print("👆 And producer is now 'custom_demo_system'!")

    transport.close()


if __name__ == "__main__":
    main()
