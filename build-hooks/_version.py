from __future__ import annotations

import enum
import functools
import re
from typing import (
    TYPE_CHECKING,
    Callable,
    Generic,
    MutableSequence,
    Optional,
    Type,
    TypeVar,
    overload,
)

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

__version_parsers__: dict = {}


def parser(alias: str):
    """
    Registers a version parser function or callable type under a given alias.

    Examples:
        >>> @parser("example")
        ... def example_parser():
        ...     return "1.0.0"
    """

    def decorator(decorated: Callable[[], str]) -> Callable[[], str]:
        __version_parsers__[alias] = decorated
        return decorated

    return decorator


def grammatical_series(*words: str) -> str:
    """
    Formats a series of words into a grammatical series.
    """
    if len(words) > 2:
        separator = ", "
        last = f'and "{words[-1]}"'
        return separator.join([*(f'"{w}"' for w in words[:-1]), last])
    elif len(words) > 1:
        return " and ".join((f'"{w}"' for w in words))
    else:
        assert words, "At least one word must be provided"
        return f'"{words[0]}"'


class NonConformingVersionString(Exception):
    """
    Exception raised when a version string does not conform to the expected
    pattern.
    """

    def __init__(self, version: Optional[str], pattern: re.Pattern):
        super().__init__(
            f"Version string must match the specified pattern, cannot parse "
            f"{repr(version)} with pattern r{repr(pattern.pattern)}."
        )


class NonConformingVersionPattern(Exception):
    """
    Exception raised when a version pattern does not define all required
    capture groups.
    """

    def __init__(self, missing_capture_groups: set[str]):
        super().__init__(
            f"Version pattern must define all required capture groups, "
            f"missing capture groups "
            f"{grammatical_series(*sorted(missing_capture_groups))}."
        )


class NumericVersionSegment(int):
    """Represents a numeric segment of a version.

    Numeric version segments behave as integers and may be incremented by
    addition, e.g. `segment += 1`.

    Args:
        value (Optional[int | str]): The numeric value of the segment.
        format (str): The format string used to render the segment.

    Raises:
        ValueError: If the value is negative.

    Examples:
        >>> segment = NumericVersionSegment(1, "{}")
        >>> repr(segment)
        '<NumericVersionSegment: 1>'
        >>> segment.render()
        '1'
    """

    def __new__(cls, value: Optional[int | str], format: str):
        if value and int(value) < 0:
            raise ValueError(f"{cls.__name__} must be positive")
        if value is None:
            value = -1
        value = max(int(value), -1)
        return super().__new__(cls, value)

    def __init__(self, value: Optional[int | str], format: str):
        if value is None:
            value = -1
        value = max(int(value), -1)
        self.__NumericVersionSegment_value__: int = value
        self.__NumericVersionSegment_format__: str = format

    def __repr__(self) -> str:
        return (
            f"<{type(self).__qualname__}: {repr(self.__NumericVersionSegment_value__)}>"
        )

    def __lt__(self, value: int | AlphanumericVersionSegment) -> bool:
        if isinstance(value, AlphanumericVersionSegment):
            return True
        return super().__lt__(value)

    def render(self) -> str:
        return (
            self.__NumericVersionSegment_format__.format(
                str(self.__NumericVersionSegment_value__)
            )
            if self.__NumericVersionSegment_value__ > -1
            else ""
        )


class AlphanumericVersionSegment(str):
    """Represents an alphanumeric segment of a version.

    Alphanumeric segments behave as strings and may contain numbers, letters,
    or dashes ("-").

    Args:
        value (Optional[str]): The alphanumeric value of the segment.
        format (str): The format string used to render the segment.

    Raises:
        ValueError: If the value is an empty string or contains invalid
        characters.

    Examples:
        >>> segment = AlphanumericVersionSegment("alpha", "-{}")
        >>> repr(segment)
        '<AlphanumericVersionSegment: "alpha">'
        >>> segment.render()
        '-alpha'
    """

    def __new__(cls, value: Optional[str], format: str, *, whitelist: str = ""):
        if value == "":
            raise ValueError(f"{cls.__name__} cannot be an empty string")
        if value is None:
            value = ""
        if any(not v.isalnum() and v not in ("-" + whitelist) for v in value):
            raise ValueError(
                f"{cls.__name__} may only contain alphanumeric characters and hyphens, got {value}"
            )
        return super().__new__(cls, value)

    def __init__(self, value: Optional[str], format: str, *, whitelist: str = ""):
        if value is None:
            value = ""
        self.__AlphanumericVersionSegment_value__: str = value
        self.__AlphanumericVersionSegment_format__: str = format
        self.__AlphanumericVersionSegment_whitelist__: str = whitelist

    def __repr__(self) -> str:
        return (
            f"<{type(self).__qualname__}: "
            f"{repr(self.__AlphanumericVersionSegment_value__)}>"
        )

    def __lt__(self, value: str | NumericVersionSegment) -> bool:
        if isinstance(value, NumericVersionSegment):
            return False
        return super().__lt__(value)

    def render(self) -> str:
        return (
            self.__AlphanumericVersionSegment_format__.format(
                str(self.__AlphanumericVersionSegment_value__)
            )
            if self.__AlphanumericVersionSegment_value__
            else ""
        )


@functools.total_ordering
class PreReleaseVersionSegment:
    """Represents a pre-release segment of a version.

    Args:
        *values (Optional[str | int]): The values of the pre-release segment.
        format (str): The format string used to render the segment.

    Examples:
        >>> segment = PreReleaseVersionSegment("alpha", 1, format="-{}")
        >>> repr(segment)
        '<PreReleaseVersionSegment: "alpha.1">'
        >>> segment.render()
        '-alpha.1'
    """

    def __init__(self, *values: Optional[str | int], format: str = "-{}"):
        self.__PreReleaseVersionSegment_value__: list[
            AlphanumericVersionSegment | NumericVersionSegment
        ] = [
            (
                NumericVersionSegment(v, format="{}")
                if isinstance(v, int)
                else AlphanumericVersionSegment(v, format="{}")
            )
            for v in values
        ]
        self.__PreReleaseVersionSegment_format__: str = format

    def __repr__(self) -> str:
        return (
            f"<{type(self).__qualname__}: "
            f"{repr('.'.join(v.render() for v in self.__PreReleaseVersionSegment_value__))}>"
        )

    def __eq__(self, other: object):
        if not isinstance(other, PreReleaseVersionSegment):
            return super().__eq__(other)
        else:
            return (
                self.__PreReleaseVersionSegment_value__
                == other.__PreReleaseVersionSegment_value__
            )

    def __lt__(self, other: PreReleaseVersionSegment) -> bool:
        if not self.__PreReleaseVersionSegment_value__:
            return False
        elif not isinstance(other, PreReleaseVersionSegment):
            return super().__lt__(other)
        elif len(self.__PreReleaseVersionSegment_value__) < len(
            other.__PreReleaseVersionSegment_value__
        ):
            return True
        else:
            for this, that in zip(
                self.__PreReleaseVersionSegment_value__,
                other.__PreReleaseVersionSegment_value__,
            ):
                if this != that:
                    if isinstance(this, NumericVersionSegment) and isinstance(
                        that, AlphanumericVersionSegment
                    ):
                        return True
                    elif isinstance(this, AlphanumericVersionSegment) and isinstance(
                        that, NumericVersionSegment
                    ):
                        return False
                    else:
                        return this < that
            return False

    def render(self) -> str:
        return (
            self.__PreReleaseVersionSegment_format__.format(
                ".".join(v.render() for v in self.__PreReleaseVersionSegment_value__)
            )
            if self.__PreReleaseVersionSegment_value__
            else ""
        )


@functools.total_ordering
class ReleaseCycle(metaclass=enum.EnumMeta):
    """Represents a release cycle of a version.

    Attributes:
        Alpha (ReleaseCycle): Alpha release cycle.
        Beta (ReleaseCycle): Beta release cycle.
        ReleaseCandidate (ReleaseCycle): Release candidate cycle.
        Production (ReleaseCycle): Production release cycle.
    """

    __ReleaseCycle_mapping__: dict[int | str, ReleaseCycle] = {}
    __ReleaseCycle_int__: Optional[int] = None
    __ReleaseCycle_str__: Optional[str] = None

    Alpha = 0, "a"
    Beta = 1, "b"
    ReleaseCandidate = 2, "rc"
    Production = 3, "."

    def __new__(cls, ordinal: int, identifier: str):
        self = object.__new__(cls)
        self.__ReleaseCycle_int__ = ordinal
        self.__ReleaseCycle_str__ = identifier
        cls.__ReleaseCycle_mapping__.update({ordinal: self, identifier: self})
        return self

    def __lt__(self, other: ReleaseCycle) -> bool:
        return int(self).__lt__(int(ReleaseCycle(other)))

    def __int__(self) -> int:
        return self.__ReleaseCycle_int__ or 0

    @classmethod
    def _missing_(cls, key: int | str) -> Optional[ReleaseCycle]:
        return cls.__ReleaseCycle_mapping__.get(key)

    def render(self) -> str:
        return self.__ReleaseCycle_str__ or str(self)


VersionSegment = Optional[
    NumericVersionSegment | AlphanumericVersionSegment | ReleaseCycle
]

segment = type("segment", (property,), {})


class VersionMeta(type):
    @property
    def parse(cls: Self) -> VersionParser[Self]:
        return VersionParser(cls)

    @property
    def segments(cls: Self) -> dict[str, segment]:
        return {k: v for k, v in cls.__dict__.items() if isinstance(v, segment)}


class Version(metaclass=VersionMeta):
    """
    Base class for version objects.

    This class provides common functionality for version objects, including
    parsing and rendering.
    """

    PATTERN: re.Pattern

    segments: dict[str, segment]

    def __init__(self):
        self._dict: dict[str, VersionSegment] = {
            k: getattr(self, k)
            for k, v in type(self).__dict__.items()
            if isinstance(v, segment)
        }

    def __iter__(self):
        return iter(self._dict)

    def __repr__(self) -> str:
        return f"<{type(self).__qualname__}: {repr(str(self))}>"

    def __str__(self) -> str:
        return "".join(v.render() for v in self.values() if v is not None)

    @overload
    def __getitem__(self, item: slice) -> dict[str, VersionSegment]: ...

    @overload
    def __getitem__(self, item: str) -> VersionSegment: ...

    def __getitem__(self, item):
        if isinstance(item, slice):
            segments = list(type(self).segments.values())
            if item.start:
                start = segments.index(item.start)
            else:
                start = None
            if item.stop:
                stop = segments.index(item.stop)
                if stop > len(segments):
                    stop = None
            else:
                stop = None
            return {k: v for k, v in list(self.items())[slice(start, stop)]}
        else:
            return self._dict[item]

    def keys(self):
        return self._dict.keys()

    def items(self):
        return self._dict.items()

    def values(self):
        return self._dict.values()


T = TypeVar("T", bound=Version)


class VersionParser(Generic[T]):
    """
    Parses version strings into version objects.

    This class is used to parse version strings into instances of the specified
    version class. It supports custom version patterns and provides a callable
    interface for parsing.

    A custom parsing pattern may optionally be specified. This pattern must
    specify all named capture groups required for the version type as defined
    by its "segment" properties.

    Registered parsers may be accessed as attributes, e.g. `parser.git()`.
    """

    def __init__(self, version_class: Type[T]):
        self.version_class = version_class

    def __call__(self, version: str, *, pattern: re.Pattern | None = None) -> T:
        if pattern is not None:
            missing_capture_groups = set(
                self.version_class.PATTERN.groupindex.keys()
            ) - set(pattern.groupindex.keys())
            if missing_capture_groups:
                raise NonConformingVersionPattern(missing_capture_groups)
        else:
            pattern = self.version_class.PATTERN
        match = pattern.match(version)
        if not match:
            raise NonConformingVersionString(version, pattern)
        segments: dict[str, str] = {
            k: v for k, v in match.groupdict().items() if v is not None
        }
        return self.version_class(**segments)

    def __getattr__(self, attribute):
        return lambda: self(__version_parsers__[attribute]())


@functools.total_ordering
class PythonicVersion(Version):
    """
    Class representing a Pythonic version as described by PEP 440.

    PEP 440 is a standard for versioning Python projects. This class provides
    methods to parse, compare, and render versions according to PEP 440.

    Examples:
        >>> version = PythonicVersion(major_release=1, minor_release=0, patch_release=0)
        >>> repr(version)
        "<PythonicVersion: '1.0.0'>"
        >>> str(version)
        '1.0.0'
        >>> version.minor_release += 1
        >>> str(version)
        '1.1.0'
    """

    PATTERN: re.Pattern = re.compile(
        r"v?"
        r"((?P<epoch>\d+)(?:!))?"
        r"(?P<major_release>\d+)?"
        r"(?:(?:\.)(?P<minor_release>\d+))?"
        r"((?P<release_cycle>\.|a|b|rc)"
        r"(?P<patch_release>\d+))?"
        r"((?:\.post)(?P<post_release>\d+))?"
        r"((?:\.dev)(?P<dev_release>\d+))?"
        r"((?:\+)(?P<local_identifier>[a-zA-Z0-9.]+))?"
        r"$"
    )

    if TYPE_CHECKING:
        parse: VersionParser[PythonicVersion]

    def __init__(
        self,
        epoch: Optional[int | str] = None,
        major_release: int | str = 0,
        minor_release: Optional[int | str] = None,
        release_cycle: Optional[int | str | ReleaseCycle] = None,
        patch_release: Optional[int | str] = None,
        post_release: Optional[int | str] = None,
        dev_release: Optional[int | str] = None,
        local_identifier: Optional[str] = None,
    ):
        assert major_release is not None, "Major release must be defined"
        if patch_release is not None:
            assert minor_release is not None, (
                "Minor release must be defined if patch release is defined"
            )
        assert (release_cycle is None) == (patch_release is None), (
            "Patch release and release cycle must be defined together"
        )
        self._epoch = epoch
        self._major_release = major_release
        self._minor_release = minor_release
        self._release_cycle = release_cycle
        self._patch_release = patch_release
        self._post_release = post_release
        self._dev_release = dev_release
        self._local_identifier = local_identifier or None
        super().__init__()

    def __eq__(self, other: object):
        if not isinstance(other, PythonicVersion):
            return super().__eq__(other)
        else:
            return all(
                (
                    self.epoch == other.epoch,
                    self.major_release == other.major_release,
                    self.minor_release == other.minor_release,
                    self.release_cycle == other.release_cycle,
                    self.patch_release == other.patch_release,
                    self.post_release == other.post_release,
                    self.dev_release == other.dev_release,
                    self.local_identifier == other.local_identifier,
                )
            )

    def __lt__(self, other: PythonicVersion) -> bool:
        if isinstance(other, PythonicVersion):
            for segment in PythonicVersion.segments:
                this, that = getattr(self, segment), getattr(other, segment)
                if this is None:
                    this = -1
                if that is None:
                    that = -1
                if this != that:
                    return this < that
            return False
        else:
            return super().__lt__(other)

    @segment
    def epoch(self) -> Optional[NumericVersionSegment]:
        return (
            self._epoch
            if self._epoch is None
            else NumericVersionSegment(self._epoch, format="{}!")
        )

    @segment
    def major_release(self) -> Optional[NumericVersionSegment]:
        return NumericVersionSegment(self._major_release, format="{}")

    @segment
    def minor_release(self) -> Optional[NumericVersionSegment]:
        return NumericVersionSegment(self._minor_release, format=".{}")

    @segment
    def release_cycle(self) -> Optional[ReleaseCycle]:
        if self._release_cycle is None:
            return self._release_cycle
        else:
            return ReleaseCycle(self._release_cycle)

    @segment
    def patch_release(self) -> Optional[NumericVersionSegment]:
        return NumericVersionSegment(self._patch_release, format="{}")

    @segment
    def post_release(self) -> Optional[NumericVersionSegment]:
        return (
            self._post_release
            if self._post_release is None
            else NumericVersionSegment(self._post_release, format=".post{}")
        )

    @segment
    def dev_release(self) -> Optional[NumericVersionSegment]:
        return (
            self._dev_release
            if self._dev_release is None
            else NumericVersionSegment(self._dev_release, format=".dev{}")
        )

    @segment
    def local_identifier(self) -> Optional[AlphanumericVersionSegment]:
        return (
            self._local_identifier
            if self._local_identifier is None
            else AlphanumericVersionSegment(
                self._local_identifier, format="+{}", whitelist="."
            )
        )

    @property
    def local(self) -> str:
        return "".join(
            v.render()
            for v in self[type(self).local_identifier :].values()
            if v is not None
        )

    @property
    def public(self) -> str:
        return "".join(
            v.render()
            for v in self[: type(self).local_identifier].values()
            if v is not None
        )


@functools.total_ordering
class SemanticVersion(Version):
    """
    Class representing a semantic version as described by SemVer 2.0.

    This class provides methods to parse, compare, and render versions
    according to the SemVer 2.0 specification.

    Examples:
        >>> version = SemanticVersion(major_release=1, minor_release=0, patch_release=0)
        >>> str(version)
        '1.0.0'
        >>> version.minor_release += 1
        >>> str(version)
        '1.1.0'
    """

    PATTERN: re.Pattern = re.compile(
        r"^v?"
        r"(?P<major_release>0|[1-9]\d*)"
        r"\.(?P<minor_release>0|[1-9]\d*)"
        r"\.(?P<patch_release>0|[1-9]\d*)"
        r"(?:-(?P<pre_release>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
        r"(?:\+(?P<build>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?"
        r"$"
    )

    def __init__(
        self,
        major_release: int | str = 0,
        minor_release: Optional[int | str] = None,
        patch_release: Optional[int | str] = None,
        pre_release: Optional[str | MutableSequence[int | str]] = None,
        build: Optional[int | str] = None,
    ):
        assert major_release is not None, "Major release must be defined"
        if patch_release is not None:
            assert minor_release is not None, (
                "Minor release must be defined if patch release is defined"
            )
        self._major_release = major_release
        self._minor_release = minor_release
        self._patch_release = patch_release
        self._pre_release = (
            pre_release.split(".")
            if isinstance(pre_release, str)
            else [pre_release]
            if isinstance(pre_release, int)
            else pre_release
        )
        self._build = build
        super().__init__()

    def __eq__(self, other):
        if not isinstance(other, SemanticVersion):
            return super().__eq__(other)
        else:
            return all(
                (
                    self.major_release == other.major_release,
                    self.minor_release == other.minor_release,
                    self.patch_release == other.patch_release,
                    self.pre_release == other.pre_release,
                    self.build == other.build,
                )
            )

    def __lt__(self, other: SemanticVersion) -> bool:
        if isinstance(other, SemanticVersion):
            for segment in SemanticVersion.segments:
                this, that = getattr(self, segment), getattr(other, segment)
                if segment == "pre_release":
                    if this is None:
                        this = PreReleaseVersionSegment(format="{}")
                    if that is None:
                        that = PreReleaseVersionSegment(format="{}")
                    if this != that:
                        return this < that
                else:
                    if this is None:
                        this = -1
                    if that is None:
                        that = -1
                    if this != that:
                        return this < that
            return False
        else:
            return super().__lt__(other)

    @segment
    def major_release(self) -> Optional[NumericVersionSegment]:
        return NumericVersionSegment(self._major_release, format="{}")

    @segment
    def minor_release(self) -> Optional[NumericVersionSegment]:
        return NumericVersionSegment(self._minor_release, format=".{}")

    @segment
    def patch_release(self) -> Optional[NumericVersionSegment]:
        return NumericVersionSegment(self._patch_release, format=".{}")

    @segment
    def pre_release(
        self,
    ) -> Optional[PreReleaseVersionSegment]:
        if self._pre_release:
            return PreReleaseVersionSegment(*self._pre_release, format="-{}")

    @segment
    def build(self) -> Optional[AlphanumericVersionSegment]:
        return (
            self._build
            if self._build is None
            else AlphanumericVersionSegment(str(self._build), format="+{}")
        )

    @property
    def local(self) -> str:
        return ""

    @property
    def public(self) -> str:
        return "".join(v.render() for v in self.values() if v is not None)
