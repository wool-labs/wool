import logging
from typing import Final

import pytest

logger = logging.getLogger(__name__)

_automark = True
_ignore_unknown = True


class DependencyItemStatus:
    """Status of a test item in a dependency manager."""

    _phases = ("setup", "call", "teardown")

    def __init__(self):
        self._results = dict.fromkeys(self._phases)

    def __str__(self):
        results = ", ".join(["%s: %s" % (w, self._results[w]) for w in self._phases])
        return f"Status({results})"

    def add_result(self, rep):
        self._results[rep.when] = rep.outcome

    @property
    def passed(self):
        return list(self._results.values()) == ["passed", "passed", "passed"]


class DependencyManager:
    """Dependency manager, stores the results of tests."""

    SCOPES: Final = {
        "session": pytest.Session,
        "package": pytest.Package,
        "module": pytest.Module,
        "class": pytest.Class,
    }

    @classmethod
    def get_manager(cls, item, scope):
        """Get the DependencyManager object from the node at scope level.
        Create it, if not yet present.
        """
        node = item.getparent(cls.SCOPES[scope])
        if not node:
            return None
        if not hasattr(node, "dependency_manager"):
            node.dependency_manager = cls(scope)
        return node.dependency_manager

    def __init__(self, scope):
        self._results = {}
        self.scope = scope

    def add_result(self, item, name, rep):
        if not name:
            # Old versions of pytest used to add an extra "::()" to
            # the node ids of class methods to denote the class
            # instance.  This has been removed in pytest 4.0.0.
            nodeid = item.nodeid.replace("::()::", "::")
            if self.scope in {"session", "package"}:
                name = nodeid
            elif self.scope == "module":
                name = nodeid.split("::", 1)[1]
            elif self.scope == "class":
                name = nodeid.split("::", 2)[2]
            else:
                raise RuntimeError("Internal error: invalid scope '%s'" % self.scope)
        status = self._results.setdefault(name, DependencyItemStatus())
        logger.debug(
            "register %s %s %s in %s scope",
            rep.when,
            name,
            rep.outcome,
            self.scope,
        )
        status.add_result(rep)

    def check_dependencies(self, dependencies, item):
        logger.debug("check dependencies of %s in %s scope ...", item.name, self.scope)
        for dependency in dependencies:
            if dependency in self._results:
                if self._results[dependency].passed:
                    logger.debug("... %s succeeded", dependency)
                    continue
                else:
                    logger.debug("... %s has not succeeded", dependency)
            else:
                logger.debug("... %s is unknown", dependency)
                if _ignore_unknown:
                    continue
            logger.metadata("skip %s because it depends on %s", item.name, dependency)
            pytest.skip("%s depends on %s" % (item.name, dependency))


def depends(request, other, scope="module"):
    """Add dependency on other test.

    Call pytest.skip() unless a successful outcome of all of the tests in
    other has been registered previously.  This has the same effect as
    the `depends` keyword argument to the :func:`pytest.mark.dependency`
    marker.  In contrast to the marker, this function may be called at
    runtime during a test.

    :param request: the value of the `request` pytest fixture related
        to the current test.
    :param other: dependencies, a list of names of tests that this
        test depends on.  The names of the dependencies must be
        adapted to the scope.
    :type other: iterable of :class:`str`
    :param scope: the scope to search for the dependencies.  Must be
        either `'session'`, `'package'`, `'module'`, or `'class'`.
    :type scope: :class:`str`
    """
    item = request.node
    manager = DependencyManager.get_manager(item, scope=scope)
    assert manager
    manager.check_dependencies(other, item)


def pytest_addoption(parser):
    parser.addini(
        "automark_dependency",
        "Add the dependency marker to all tests automatically",
        type="bool",
        default=False,
    )
    parser.addoption(
        "--ignore-unknown-dependency",
        action="store_true",
        default=False,
        help="ignore dependencies whose outcome is not known",
    )


def pytest_configure(config):
    global _automark, _ignore_unknown
    _automark = config.getini("automark_dependency")
    _ignore_unknown = config.getoption("--ignore-unknown-dependency")
    config.addinivalue_line(
        "markers",
        "dependency(name=None, depends=[]): "
        "mark a test to be used as a dependency for "
        "other tests or to depend on other tests.",
    )


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, _):
    """Store the test outcome if this item is marked "dependency"."""
    outcome = yield
    marker = item.get_closest_marker("dependency")
    if marker is not None or _automark:
        rep = outcome.get_result()
        name = marker.kwargs.get("name") if marker is not None else None
        for scope in DependencyManager.SCOPES:
            manager = DependencyManager.get_manager(item, scope=scope)
            if manager:
                manager.add_result(item, name, rep)


def pytest_runtest_setup(item):
    """Check dependencies if this item is marked "dependency".
    Skip if any of the dependencies has not been run successfully.
    """
    marker = item.get_closest_marker("dependency")
    if marker is not None:
        dependencies = marker.args
        if dependencies:
            scope = marker.kwargs.get("scope", "module")
            manager = DependencyManager.get_manager(item, scope=scope)
            assert manager
            manager.check_dependencies(dependencies, item)
