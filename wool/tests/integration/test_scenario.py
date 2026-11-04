"""Unit tests for the Scenario model."""

from dataclasses import fields

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from .conftest import _OPTIONAL_DIMENSIONS
from .conftest import BackpressureMode
from .conftest import ContextVarPattern
from .conftest import CredentialType
from .conftest import DiscoveryFactory
from .conftest import LazyMode
from .conftest import LbFactory
from .conftest import PoolMode
from .conftest import QuorumMode
from .conftest import RoutineBinding
from .conftest import RoutineShape
from .conftest import Scenario
from .conftest import StrictWarnings
from .conftest import TimeoutKind
from .conftest import WorkerOptionsKind
from .conftest import scenarios_strategy


@st.composite
def partial_scenarios(draw):
    """Draw a scenario with an arbitrary subset of its dimensions set.

    Takes the dimension values from a complete scenario so every member
    drawn is one the filter admits, then keeps a drawn subset of the
    fields and leaves the rest ``None``. ``strict_warnings`` is drawn
    separately because complete scenarios leave it unset.
    """
    complete = draw(scenarios_strategy())
    names = [f.name for f in fields(Scenario)]
    values = {name: getattr(complete, name) for name in names}
    values["strict_warnings"] = draw(st.sampled_from(list(StrictWarnings)))
    kept = draw(st.lists(st.sampled_from(names), unique=True))
    return Scenario(**{name: values[name] for name in kept})


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

    @settings(max_examples=50, deadline=None)
    @given(scenario=partial_scenarios())
    def test___or___should_return_an_equal_scenario_when_merged_with_an_empty_one(
        self, scenario
    ):
        """Test the empty scenario is an identity for the merge.

        Given:
            Any partial scenario, from none of its dimensions set to
            all of them.
        When:
            It is merged with an all-``None`` scenario, on either side.
        Then:
            It should equal the original, since the merge takes the
            right side only where it is set.
        """
        # Arrange
        empty = Scenario()

        # Act
        right = scenario | empty
        left = empty | scenario

        # Assert
        assert right == scenario
        assert left == scenario

    @settings(max_examples=50, deadline=None)
    @given(scenario=partial_scenarios())
    def test___or___should_return_an_equal_scenario_when_merged_with_itself(
        self, scenario
    ):
        """Test the merge is idempotent.

        Given:
            Any partial scenario, from none of its dimensions set to
            all of them.
        When:
            It is merged with itself.
        Then:
            It should equal the original and raise no conflict, since
            every field agrees with itself.
        """
        # Act
        merged = scenario | scenario

        # Assert
        assert merged == scenario

    def test_is_complete_with_all_fields(self):
        """Test that a fully populated scenario reports complete.

        Given:
            A scenario with all 14 dimensions set.
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
            quorum=QuorumMode.DEFAULT,
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

    def test_is_complete_should_return_true_when_strict_warnings_unset(self):
        """Test that the optional dimension is excluded from completeness.

        Given:
            A scenario with all 14 required dimensions set and the
            optional ``strict_warnings`` dimension left unset.
        When:
            ``is_complete`` is checked.
        Then:
            It should return True — the optional documentation
            dimension is not required configuration.
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
            quorum=QuorumMode.DEFAULT,
        )

        # Act & assert
        assert scenario.strict_warnings is None
        assert scenario.is_complete is True

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
        assert result == "COROUTINE-DEFAULT-_-_-_-_-_-_-_-_-_-_-_-_"

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
        assert result == "-".join(["_"] * 14)

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
            quorum=QuorumMode.DEFAULT,
        )

        # Act
        result = str(scenario)

        # Assert
        assert result == (
            "COROUTINE-DEFAULT-LOCAL_DIRECT-CLASS_REF-INSECURE-DEFAULT-"
            "NONE-MODULE_FUNCTION-EAGER-NONE-ROUND_TRIP-LOCAL_RESET-PER_YIELD-DEFAULT"
        )

    def test___str___should_append_a_segment_when_strict_warnings_set(self):
        """Test string representation when the optional dimension is set.

        Given:
            A scenario with every required dimension set plus the
            optional ``strict_warnings`` dimension.
        When:
            Converted to string.
        Then:
            The result should carry a fifteenth segment naming the
            optional member, appended after the required ones.
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
            quorum=QuorumMode.DEFAULT,
            strict_warnings=StrictWarnings.ALL_DECODABLE,
        )

        # Act
        result = str(scenario)

        # Assert
        assert len(result.split("-")) == 15
        assert result == (
            "COROUTINE-DEFAULT-LOCAL_DIRECT-CLASS_REF-INSECURE-DEFAULT-"
            "NONE-MODULE_FUNCTION-EAGER-NONE-ROUND_TRIP-LOCAL_RESET-PER_YIELD-"
            "DEFAULT-ALL_DECODABLE"
        )

    @settings(max_examples=50, deadline=None)
    @given(scenario=partial_scenarios())
    def test___str___should_carry_one_segment_per_set_or_required_dimension(
        self, scenario
    ):
        """Test the string form maps dimensions onto segments one for one.

        Given:
            Any partial scenario, from none of its dimensions set to
            all of them.
        When:
            It is converted to a string.
        Then:
            It should carry one dash-separated segment for every
            required dimension and for every optional one that is set,
            each naming the member it holds or ``_`` where it is unset,
            so a pytest ID identifies its scenario uniquely.
        """
        # Arrange
        expected = [
            value.name if (value := getattr(scenario, f.name)) is not None else "_"
            for f in fields(Scenario)
            if getattr(scenario, f.name) is not None
            or f.name not in _OPTIONAL_DIMENSIONS
        ]

        # Act
        rendered = str(scenario)

        # Assert
        assert rendered.split("-") == expected
