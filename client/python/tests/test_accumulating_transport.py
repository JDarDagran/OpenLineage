# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
Tests for AccumulatingTransport.

This module contains comprehensive tests for the AccumulatingTransport class
and its various aggregation strategies and emission policies.
"""

import time
import uuid
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
from openlineage.client.facet_v2 import parent_run
from openlineage.client.generated.base import Job, Run
from openlineage.client.run import InputDataset, OutputDataset, RunEvent, RunState
from openlineage.client.transport.accumulating import (
    AccumulatedEvent,
    AccumulatingConfig,
    AccumulatingTransport,
    BatchAggregationStrategy,
    CompletionBasedEmissionPolicy,
    CountBasedEmissionPolicy,
    EventGroup,
    HybridEmissionPolicy,
    MergeDatasetsAggregationStrategy,
    ParentChildAggregationStrategy,
    TimeBasedEmissionPolicy,
)


class TestAggregationStrategies:
    """Test aggregation strategy implementations."""

    def test_parent_child_strategy_grouping(self):
        """Test parent-child strategy event grouping."""
        strategy = ParentChildAggregationStrategy({})

        # Parent event
        parent_event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId="parent-123"),
            job=Job(namespace="test", name="parent"),
            inputs=[],
            outputs=[],
            producer="test",
        )

        # Child event with parent facet
        child_event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(
                runId="child-456",
                facets={
                    "parent": parent_run.ParentRunFacet(
                        run=parent_run.Run(runId="parent-123"),
                        job=parent_run.Job(namespace="test", name="parent"),
                    )
                },
            ),
            job=Job(namespace="test", name="child"),
            inputs=[],
            outputs=[],
            producer="test",
        )

        # Test grouping
        parent_key = strategy.get_group_key(parent_event)
        child_key = strategy.get_group_key(child_event)

        assert parent_key == "run:parent-123"
        assert child_key == "parent:parent-123"  # Child grouped by parent ID

    def test_parent_child_strategy_aggregation(self):
        """Test parent-child event aggregation."""
        strategy = ParentChildAggregationStrategy({"merge_duplicate_datasets": True})

        parent_run_id = "parent-123"
        child_run_id = "child-456"

        # Create test datasets
        input_ds = InputDataset(namespace="test", name="input", facets={})
        output_ds = OutputDataset(namespace="test", name="output", facets={})

        # Child events with datasets
        child_events = [
            AccumulatedEvent(
                event=RunEvent(
                    eventType=RunState.COMPLETE,
                    eventTime=datetime.now(timezone.utc).isoformat(),
                    run=Run(
                        runId=child_run_id,
                        facets={
                            "parent": parent_run.ParentRunFacet(
                                run=parent_run.Run(runId=parent_run_id),
                                job=parent_run.Job(namespace="test", name="parent"),
                            )
                        },
                    ),
                    job=Job(namespace="test", name="child"),
                    inputs=[input_ds],
                    outputs=[output_ds],
                    producer="test",
                )
            )
        ]

        # Parent event
        parent_events = [
            AccumulatedEvent(
                event=RunEvent(
                    eventType=RunState.COMPLETE,
                    eventTime=datetime.now(timezone.utc).isoformat(),
                    run=Run(runId=parent_run_id),
                    job=Job(namespace="test", name="parent"),
                    inputs=[],
                    outputs=[],
                    producer="test",
                )
            )
        ]

        all_events = child_events + parent_events
        aggregated = strategy.aggregate_events(all_events)

        # Should return enhanced parent events
        assert len(aggregated) == 1
        parent_result = aggregated[0]
        assert parent_result.run.runId == parent_run_id
        assert len(parent_result.inputs) == 1
        assert len(parent_result.outputs) == 1
        assert parent_result.inputs[0].name == "input"
        assert parent_result.outputs[0].name == "output"

    def test_merge_datasets_strategy(self):
        """Test dataset merging strategy."""
        strategy = MergeDatasetsAggregationStrategy({"group_by_job": True})

        # Events with same job but different facets
        job = Job(namespace="test", name="shared_job")
        events = []

        for i in range(2):
            dataset = OutputDataset(
                namespace="test", name="shared_dataset", facets={f"facet_{i}": {"value": i}}
            )

            event = RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=job,
                inputs=[],
                outputs=[dataset],
                producer="test",
            )
            events.append(AccumulatedEvent(event=event))

        # Test grouping - should be same group
        key1 = strategy.get_group_key(events[0].event)
        key2 = strategy.get_group_key(events[1].event)
        assert key1 == key2

        # Test aggregation - should merge facets
        aggregated = strategy.aggregate_events(events)
        assert len(aggregated) == 1

        result_event = aggregated[0]
        assert len(result_event.outputs) == 1
        merged_dataset = result_event.outputs[0]
        assert "facet_0" in merged_dataset.facets
        assert "facet_1" in merged_dataset.facets

    def test_batch_strategy(self):
        """Test batch aggregation strategy."""
        strategy = BatchAggregationStrategy({"group_all": True})

        # Create different types of events
        events = []
        for i in range(3):
            event = RunEvent(
                eventType=RunState.START if i % 2 == 0 else RunState.COMPLETE,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=Job(namespace="test", name=f"job_{i}"),
                inputs=[],
                outputs=[],
                producer="test",
            )
            events.append(AccumulatedEvent(event=event))

        # All should have same group key
        keys = [strategy.get_group_key(e.event) for e in events]
        assert all(k == "batch" for k in keys)

        # Aggregation should return original events unchanged
        aggregated = strategy.aggregate_events(events)
        assert len(aggregated) == 3
        for i, event in enumerate(aggregated):
            assert event.job.name == f"job_{i}"


class TestEmissionPolicies:
    """Test emission policy implementations."""

    def test_time_based_policy(self):
        """Test time-based emission policy."""
        policy = TimeBasedEmissionPolicy({"trigger_interval": 1.0, "max_age": 2.0})
        # Create a group with old events
        group = EventGroup(group_key="test")
        old_time = time.time() - 3.0  # 3 seconds ago
        group.first_event_time = old_time

        # Should trigger due to max_age
        assert policy.should_emit(group, time.time())

        # Create a fresh group
        group = EventGroup(group_key="test")
        group.first_event_time = time.time()

        # Should not trigger yet
        assert not policy.should_emit(group, time.time())

        # Wait and check interval trigger
        time.sleep(0.1)
        policy.last_emission = time.time() - 1.5  # Simulate last emission 1.5s ago
        assert policy.should_emit(group, time.time())

    def test_count_based_policy(self):
        """Test count-based emission policy."""
        policy = CountBasedEmissionPolicy({"max_events": 3})
        group = EventGroup(group_key="test")

        # Add events one by one
        for i in range(2):
            event = AccumulatedEvent(
                event=RunEvent(
                    eventType=RunState.START,
                    eventTime=datetime.now(timezone.utc).isoformat(),
                    run=Run(runId=str(uuid.uuid4())),
                    job=Job(namespace="test", name="job"),
                    inputs=[],
                    outputs=[],
                    producer="test",
                )
            )
            group.add_event(event)
            # Should not trigger yet
            assert not policy.should_emit(group, time.time())

        # Add third event
        event = AccumulatedEvent(
            event=RunEvent(
                eventType=RunState.START,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=Job(namespace="test", name="job"),
                inputs=[],
                outputs=[],
                producer="test",
            )
        )
        group.add_event(event)

        # Should trigger now
        assert policy.should_emit(group, time.time())

    def test_completion_based_policy(self):
        """Test completion-based emission policy."""
        policy = CompletionBasedEmissionPolicy({"completion_event_types": ["COMPLETE", "FAIL"]})
        group = EventGroup(group_key="test")

        # Add START event - should not trigger
        start_event = AccumulatedEvent(
            event=RunEvent(
                eventType=RunState.START,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=Job(namespace="test", name="job"),
                inputs=[],
                outputs=[],
                producer="test",
            )
        )
        group.add_event(start_event)
        assert not policy.should_emit(group, time.time())

        # Add COMPLETE event - should trigger
        complete_event = AccumulatedEvent(
            event=RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=Job(namespace="test", name="job"),
                inputs=[],
                outputs=[],
                producer="test",
            )
        )
        group.add_event(complete_event)
        assert policy.should_emit(group, time.time())

    def test_hybrid_policy_or(self):
        """Test hybrid policy with OR logic."""
        policy = HybridEmissionPolicy(
            {
                "operator": "OR",
                "policies": [
                    {"type": "count_based", "max_events": 5},
                    {"type": "completion_based", "completion_event_types": ["COMPLETE"]},
                ],
            }
        )
        group = EventGroup(group_key="test")

        # Add COMPLETE event - should trigger via completion policy
        complete_event = AccumulatedEvent(
            event=RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=Job(namespace="test", name="job"),
                inputs=[],
                outputs=[],
                producer="test",
            )
        )
        group.add_event(complete_event)
        assert policy.should_emit(group, time.time())

    def test_hybrid_policy_and(self):
        """Test hybrid policy with AND logic."""
        policy = HybridEmissionPolicy(
            {
                "operator": "AND",
                "policies": [
                    {"type": "count_based", "max_events": 2},
                    {"type": "completion_based", "completion_event_types": ["COMPLETE"]},
                ],
            }
        )
        group = EventGroup(group_key="test")

        # Add COMPLETE event but not enough count - should not trigger
        complete_event = AccumulatedEvent(
            event=RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=Job(namespace="test", name="job"),
                inputs=[],
                outputs=[],
                producer="test",
            )
        )
        group.add_event(complete_event)
        assert not policy.should_emit(group, time.time())

        # Add another event - now should trigger
        start_event = AccumulatedEvent(
            event=RunEvent(
                eventType=RunState.START,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=Job(namespace="test", name="job"),
                inputs=[],
                outputs=[],
                producer="test",
            )
        )
        group.add_event(start_event)
        assert policy.should_emit(group, time.time())


class TestAccumulatingTransport:
    """Test AccumulatingTransport integration."""

    def test_transport_initialization(self):
        """Test transport initialization with different configs."""
        # Test with parent-child strategy
        config = AccumulatingConfig(
            strategy="parent_child",
            emission_policy="count_based",
            emission_config={"max_events": 5},
            transport={"type": "console"},
        )

        transport = AccumulatingTransport(config)
        assert transport.config.strategy == "parent_child"
        assert isinstance(transport._strategy, ParentChildAggregationStrategy)
        assert isinstance(transport._emission_policy, CountBasedEmissionPolicy)

    def test_direct_emission_for_non_accumulated_events(self):
        """Test that non-accumulated events are emitted directly."""
        mock_transport = Mock()
        config = AccumulatingConfig(
            strategy="parent_child",  # Only accumulates RunEvents
            emission_policy="count_based",
            transport={"type": "console"},
        )

        transport = AccumulatingTransport(config)
        transport._underlying_transport = mock_transport

        # Create a non-RunEvent (custom event)
        class CustomEvent:
            pass

        custom_event = CustomEvent()
        transport.emit(custom_event)

        # Should be emitted directly
        mock_transport.emit.assert_called_once_with(custom_event)

    def test_accumulation_and_emission(self):
        """Test event accumulation and emission."""
        mock_transport = Mock()
        config = AccumulatingConfig(
            strategy="batch",
            emission_policy="count_based",
            emission_config={"max_events": 2},
            transport={"type": "console"},
        )

        transport = AccumulatingTransport(config)
        transport._underlying_transport = mock_transport

        # Emit events
        for i in range(3):
            event = RunEvent(
                eventType=RunState.START,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=Job(namespace="test", name=f"job_{i}"),
                inputs=[],
                outputs=[],
                producer="test",
            )
            transport.emit(event)

        # Should have emitted after 2nd event due to count policy
        assert mock_transport.emit.call_count >= 2

    def test_flush_functionality(self):
        """Test manual flush of accumulated events."""
        mock_transport = Mock()
        config = AccumulatingConfig(
            strategy="batch",
            emission_policy="count_based",
            emission_config={"max_events": 10},  # High threshold
            transport={"type": "console"},
        )

        transport = AccumulatingTransport(config)
        transport._underlying_transport = mock_transport

        # Emit events that won't trigger automatic emission
        for i in range(3):
            event = RunEvent(
                eventType=RunState.START,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=Job(namespace="test", name=f"job_{i}"),
                inputs=[],
                outputs=[],
                producer="test",
            )
            transport.emit(event)

        # No emissions yet
        mock_transport.emit.assert_not_called()

        # Manual flush
        transport.flush()

        # Should have emitted all events
        assert mock_transport.emit.call_count == 3

    def test_error_handling_with_fallback(self):
        """Test error handling with fallback to direct emission."""
        mock_transport = Mock()

        # Configure transport to continue on error with fallback
        config = AccumulatingConfig(
            strategy="parent_child",
            emission_policy="count_based",
            emission_config={"max_events": 1},
            transport={"type": "console"},
            continue_on_aggregation_error=True,
            fallback_to_direct_emission=True,
        )

        transport = AccumulatingTransport(config)
        transport._underlying_transport = mock_transport

        # Mock strategy to raise error
        transport._strategy.aggregate_events = Mock(side_effect=Exception("Test error"))

        # Emit event
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=str(uuid.uuid4())),
            job=Job(namespace="test", name="job"),
            inputs=[],
            outputs=[],
            producer="test",
        )

        # Should not raise exception
        transport.emit(event)

        # Should have called fallback emission
        mock_transport.emit.assert_called()

    def test_buffer_size_limits(self):
        """Test buffer size limit enforcement."""
        config = AccumulatingConfig(
            strategy="batch",
            emission_policy="count_based",
            emission_config={"max_events": 1000},  # High threshold to prevent emission
            transport={"type": "console"},
            max_buffer_size=5,  # Small buffer
        )

        transport = AccumulatingTransport(config)

        # Emit more events than buffer size
        for i in range(10):
            event = RunEvent(
                eventType=RunState.START,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=Job(namespace="test", name=f"job_{i}"),
                inputs=[],
                outputs=[],
                producer="test",
            )
            transport.emit(event)

        # Check that buffer size is enforced
        total_events = sum(len(group.events) for group in transport._event_groups.values())
        assert total_events <= config.max_buffer_size

    def test_stats_reporting(self):
        """Test statistics reporting."""
        config = AccumulatingConfig(
            strategy="batch",
            emission_policy="count_based",
            emission_config={"max_events": 100},
            transport={"type": "console"},
        )

        transport = AccumulatingTransport(config)

        # Emit some events
        for i in range(3):
            event = RunEvent(
                eventType=RunState.START,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=Run(runId=str(uuid.uuid4())),
                job=Job(namespace="test", name=f"job_{i}"),
                inputs=[],
                outputs=[],
                producer="test",
            )
            transport.emit(event)

        stats = transport.get_stats()
        assert "accumulated_groups" in stats
        assert "accumulated_events" in stats
        assert "strategy" in stats
        assert "emission_policy" in stats
        assert stats["accumulated_events"] == 3
        assert stats["strategy"] == "batch"


class TestCustomStrategies:
    """Test custom strategy support."""

    def test_custom_strategy_instance(self):
        """Test providing a custom strategy instance."""

        class TestCustomStrategy:
            def __init__(self, config):
                self.config = config
                self.get_group_key_called = False
                self.should_accumulate_called = False
                self.aggregate_events_called = False

            def get_group_key(self, event):
                self.get_group_key_called = True
                return "test_group"

            def should_accumulate(self, event):
                self.should_accumulate_called = True
                return isinstance(event, RunEvent)

            def aggregate_events(self, events):
                self.aggregate_events_called = True
                return [acc_event.event for acc_event in events]

        # Create custom strategy instance
        custom_strategy = TestCustomStrategy({"test_param": "value"})

        config = AccumulatingConfig(
            strategy=custom_strategy,  # Pass instance directly
            emission_policy="count_based",
            emission_config={"max_events": 1},
            transport={"type": "console"},
        )

        transport = AccumulatingTransport(config)

        # Verify strategy was used
        assert transport._strategy is custom_strategy

        # Emit an event to trigger strategy methods
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId="test-123"),
            job=Job(namespace="test", name="test_job"),
            inputs=[],
            outputs=[],
            producer="test",
        )

        transport.emit(event)
        transport.close()

        # Verify all strategy methods were called
        assert custom_strategy.get_group_key_called
        assert custom_strategy.should_accumulate_called
        assert custom_strategy.aggregate_events_called

    def test_custom_strategy_class(self):
        """Test providing a custom strategy class."""

        class TestCustomStrategy:
            def __init__(self, config):
                self.config = config
                self.instantiated_with_config = True

            def get_group_key(self, event):
                return f"custom:{event.job.name}"

            def should_accumulate(self, event):
                return True

            def aggregate_events(self, events):
                return [acc_event.event for acc_event in events]

        config = AccumulatingConfig(
            strategy=TestCustomStrategy,  # Pass class
            strategy_config={"custom_param": "test_value"},
            emission_policy="count_based",
            emission_config={"max_events": 1},
            transport={"type": "console"},
        )

        transport = AccumulatingTransport(config)

        # Verify strategy was instantiated correctly
        assert isinstance(transport._strategy, TestCustomStrategy)
        assert transport._strategy.instantiated_with_config
        assert transport._strategy.config["custom_param"] == "test_value"

        transport.close()

    def test_custom_strategy_factory(self):
        """Test providing a custom strategy factory function."""

        class TestCustomStrategy:
            def __init__(self, config):
                self.config = config
                self.created_by_factory = True

            def get_group_key(self, event):
                return "factory_group"

            def should_accumulate(self, event):
                return True

            def aggregate_events(self, events):
                return [acc_event.event for acc_event in events]

        def custom_strategy_factory(config):
            """Factory function that creates strategy instances."""
            strategy = TestCustomStrategy(config)
            strategy.factory_param = config.get("factory_param", "default")
            return strategy

        config = AccumulatingConfig(
            strategy=custom_strategy_factory,  # Pass factory function
            strategy_config={"factory_param": "test_factory_value"},
            emission_policy="count_based",
            emission_config={"max_events": 1},
            transport={"type": "console"},
        )

        transport = AccumulatingTransport(config)

        # Verify strategy was created by factory
        assert isinstance(transport._strategy, TestCustomStrategy)
        assert transport._strategy.created_by_factory
        assert transport._strategy.factory_param == "test_factory_value"

        transport.close()

    def test_invalid_custom_strategy_types(self):
        """Test error handling for invalid custom strategy types."""

        # Test invalid type
        with pytest.raises(ValueError, match="Strategy must be a string name"):
            AccumulatingConfig(strategy=123)

        # Test invalid factory return type
        def bad_factory(config):
            return "not a strategy"

        config = AccumulatingConfig(
            strategy=bad_factory,
            strategy_config={},
            emission_policy="count_based",
            emission_config={"max_events": 1},
            transport={"type": "console"},
        )

        with pytest.raises(
            ValueError, match="Custom strategy factory must return AggregationStrategyInterface"
        ):
            AccumulatingTransport(config)

    def test_custom_strategy_with_inheritance(self):
        """Test custom strategy that properly inherits from AggregationStrategyInterface."""

        from openlineage.client.transport.accumulating import AggregationStrategyInterface

        class ProperCustomStrategy(AggregationStrategyInterface):
            def __init__(self, config):
                self.config = config

            def get_group_key(self, event):
                return f"proper:{event.run.runId}"

            def should_accumulate(self, event):
                return isinstance(event, RunEvent)

            def aggregate_events(self, events):
                # Custom aggregation: add prefix to producer
                result = []
                for acc_event in events:
                    event = acc_event.event
                    if hasattr(event, "producer"):
                        # Create modified event
                        modified_event = RunEvent(
                            eventType=event.eventType,
                            eventTime=event.eventTime,
                            run=event.run,
                            job=event.job,
                            inputs=event.inputs if hasattr(event, "inputs") else [],
                            outputs=event.outputs if hasattr(event, "outputs") else [],
                            producer=f"aggregated-{event.producer}",
                        )
                        result.append(modified_event)
                    else:
                        result.append(event)
                return result

        config = AccumulatingConfig(
            strategy=ProperCustomStrategy,
            strategy_config={"test": "value"},
            emission_policy="count_based",
            emission_config={"max_events": 1},
            transport={"type": "console"},
        )

        transport = AccumulatingTransport(config)

        # Verify proper inheritance
        assert isinstance(transport._strategy, AggregationStrategyInterface)
        assert isinstance(transport._strategy, ProperCustomStrategy)

        transport.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
