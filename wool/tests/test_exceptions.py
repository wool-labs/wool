import pytest

from wool import AdvertiseHostError
from wool import IneffectiveLeaseWarning
from wool import IneffectiveQuorumTimeoutWarning
from wool import LoopbackAdvertisementWarning
from wool import SerializationWarning
from wool import WoolError
from wool import WoolWarning


class TestWoolError:
    """Tests for WoolError umbrella membership.

    Fully qualified name: wool.exceptions.WoolError
    """

    def test___init___should_subclass_wool_error(self):
        """Test wool's typed exceptions descend from WoolError.

        Given:
            Wool's typed runtime exception classes
        When:
            Checked against WoolError with issubclass
        Then:
            It should match every one, so a single except clause
            catches all wool-raised errors.
        """
        # Act & assert
        assert issubclass(AdvertiseHostError, WoolError)


class TestWoolWarning:
    """Tests for WoolWarning umbrella membership.

    Fully qualified name: wool.exceptions.WoolWarning
    """

    @pytest.mark.parametrize(
        "category",
        [
            IneffectiveLeaseWarning,
            IneffectiveQuorumTimeoutWarning,
            LoopbackAdvertisementWarning,
            SerializationWarning,
        ],
    )
    def test___init___should_subclass_wool_warning_and_not_stdlib_warnings(
        self, category
    ):
        """Test wool's warning categories descend from WoolWarning.

        Given:
            A wool warning category
        When:
            Checked against WoolWarning with issubclass
        Then:
            It should match, so a single filter on WoolWarning governs
            every wool warning, and it should not descend from
            UserWarning or RuntimeWarning — wool warnings are pure
            WoolWarning subclasses.
        """
        # Act & assert
        assert issubclass(category, WoolWarning)
        assert not issubclass(category, (UserWarning, RuntimeWarning))
