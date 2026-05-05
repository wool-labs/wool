"""Unit tests for the Scenario model."""

import pytest

from .conftest import BackpressureMode
from .conftest import ContextVarPattern
from .conftest import CredentialType
from .conftest import DiscoveryFactory
from .conftest import LazyMode
from .conftest import LbFactory
from .conftest import PoolMode
from .conftest import RoutineBinding
from .conftest import RoutineShape
from .conftest import Scenario
from .conftest import TimeoutKind
from .conftest import WorkerOptionsKind


@pytest.mark.integration
class TestScenario:
    def test___or___with_disjoint_fields(self):
        """Test merging two partial scenarios with disjoint fields.

        Given:
            Two partial scenarios with non-overlapping dimensions set.
        When:
            They are merged with the ``|`` operator.
        Then:
            It should produce a combined scenario with both sides' fields.
        """
        # Arrange
        left = Scenario(shape=RoutineShape.COROUTINE)
        right = Scenario(pool_mode=PoolMode.DEFAULT)

        # Act
        merged = left | right

        # Assert
        assert merged.shape is RoutineShape.COROUTINE
        assert merged.pool_mode is PoolMode.DEFAULT

    def test___or___with_conflicting_fields(self):
        """Test merging two scenarios that set the same field differently.

        Given:
            Two scenarios that both set ``shape`` to different values.
        When:
            They are merged with the ``|`` operator.
        Then:
            It should raise ValueError.
        """
        # Arrange
        left = Scenario(shape=RoutineShape.COROUTINE)
        right = Scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)

        # Act & assert
        with pytest.raises(ValueError, match="Conflicting values for shape"):
            left | right

    def test___or___with_identical_values(self):
        """Test merging two scenarios that set the same field identically.

        Given:
            Two scenarios that both set ``shape`` to the same value.
        When:
            They are merged with the ``|`` operator.
        Then:
            It should merge without error and preserve the value.
        """
        # Arrange
        left = Scenario(shape=RoutineShape.COROUTINE)
        right = Scenario(shape=RoutineShape.COROUTINE)

        # Act
        merged = left | right

        # Assert
        assert merged.shape is RoutineShape.COROUTINE

    def test___or___with_empty_scenario(self):
        """Test merging a partial scenario with an all-None scenario.

        Given:
            A partial scenario and a default empty scenario.
        When:
            They are merged with the ``|`` operator.
        Then:
            It should return the original non-None fields unchanged.
        """
        # Arrange
        left = Scenario(
            shape=RoutineShape.COROUTINE,
            pool_mode=PoolMode.DEFAULT,
        )
        right = Scenario()

        # Act
        merged = left | right

        # Assert
        assert merged.shape is RoutineShape.COROUTINE
        assert merged.pool_mode is PoolMode.DEFAULT
        assert merged.discovery is None

    def test_is_complete_with_all_fields(self):
        """Test that a fully populated scenario reports complete.

        Given:
            A scenario with all 13 dimensions set.
        When:
            ``is_complete`` is checked.
        Then:
            It should return True.
        """
        # Arrange
        scenario = Scenario(
            shape=RoutineShape.COROUTINE,
            pool_mode=PoolMode.DEFAULT,
            discovery=DiscoveryFactory.NONE,
            lb=LbFactory.CLASS_REF,
            credential=CredentialType.INSECURE,
            options=WorkerOptionsKind.DEFAULT,
            timeout=TimeoutKind.NONE,
            binding=RoutineBinding.MODULE_FUNCTION,
            lazy=LazyMode.LAZY,
            backpressure=BackpressureMode.NONE,
            ctx_var_1=ContextVarPattern.NONE,
            ctx_var_2=ContextVarPattern.NONE,
            ctx_var_3=ContextVarPattern.NONE,
        )

        # Act & assert
        assert scenario.is_complete is True

    def test_is_complete_with_missing_field(self):
        """Test that a partial scenario reports incomplete.

        Given:
            A scenario with only some dimensions set.
        When:
            ``is_complete`` is checked.
        Then:
            It should return False.
        """
        # Arrange
        scenario = Scenario(
            shape=RoutineShape.COROUTINE,
            pool_mode=PoolMode.DEFAULT,
        )

        # Act & assert
        assert scenario.is_complete is False

    def test___str___with_partial_fields(self):
        """Test string representation with some fields set.

        Given:
            A partial scenario with two dimensions set.
        When:
            Converted to string.
        Then:
            It should return dash-separated names with underscores
            for unset fields.
        """
        # Arrange
        scenario = Scenario(
            shape=RoutineShape.COROUTINE,
            pool_mode=PoolMode.DEFAULT,
        )

        # Act
        result = str(scenario)

        # Assert
        assert result == "COROUTINE-DEFAULT-_-_-_-_-_-_-_-_-_-_-_"

    def test___str___with_empty_scenario(self):
        """Test string representation when no dimensions are set.

        Given:
            A scenario with every dimension left at the default None.
        When:
            Converted to string.
        Then:
            The result should be an underscore per dimension joined
            by dashes --- every field renders as the unset marker.
        """
        # Arrange
        scenario = Scenario()

        # Act
        result = str(scenario)

        # Assert
        assert result == "-".join(["_"] * 13)

    def test___str___with_all_fields_set(self):
        """Test string representation when every dimension is set.

        Given:
            A scenario populated with one concrete enum member per
            dimension.
        When:
            Converted to string.
        Then:
            The result should carry each member's ``name`` joined by
            dashes, in field order, with no underscores remaining.
        """
        # Arrange
        scenario = Scenario(
            shape=RoutineShape.COROUTINE,
            pool_mode=PoolMode.DEFAULT,
            discovery=DiscoveryFactory.LOCAL_DIRECT,
            lb=LbFactory.CLASS_REF,
            credential=CredentialType.INSECURE,
            options=WorkerOptionsKind.DEFAULT,
            timeout=TimeoutKind.NONE,
            binding=RoutineBinding.MODULE_FUNCTION,
            lazy=LazyMode.EAGER,
            backpressure=BackpressureMode.NONE,
            ctx_var_1=ContextVarPattern.ROUND_TRIP,
            ctx_var_2=ContextVarPattern.LOCAL_RESET,
            ctx_var_3=ContextVarPattern.PER_YIELD,
        )

        # Act
        result = str(scenario)

        # Assert
        assert result == (
            "COROUTINE-DEFAULT-LOCAL_DIRECT-CLASS_REF-INSECURE-DEFAULT-"
            "NONE-MODULE_FUNCTION-EAGER-NONE-ROUND_TRIP-LOCAL_RESET-PER_YIELD"
        )
