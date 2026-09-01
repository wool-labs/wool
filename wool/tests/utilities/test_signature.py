import functools

import pytest

from wool.utilities.signature import accepts_kwarg
from wool.utilities.signature import presupplies_kwarg
from wool.utilities.signature import requires_kwarg
from wool.utilities.signature import unbindable_call


def _declaring(*args: str, flavor: str | None = None) -> None:
    """Return a callable declaring a keyword-only parameter."""


class _Callable:
    """A callable object declaring the parameter on its __call__."""

    def __call__(self, *args: str, flavor: str | None = None) -> None: ...


class TestAcceptsKwarg:
    """Test suite for `accepts_kwarg`."""

    @pytest.mark.parametrize(
        ("build", "expected"),
        [
            (lambda: _declaring, True),
            (lambda: _Callable(), True),
            (lambda: lambda *args, **kwargs: None, True),
            (lambda: lambda *args: None, False),
            (lambda: min, False),
        ],
        ids=["keyword-only", "callable-object", "kwargs-sink", "absent", "builtin"],
    )
    def test_accepts_kwarg_should_answer_whether_the_keyword_can_be_passed(
        self, build, expected
    ):
        """Test the predicate reports deliverability, not intent.

        Given:
            A callable that declares the keyword, absorbs it through
            ``**kwargs``, never names it, or cannot be inspected.
        When:
            It is asked whether the keyword can be passed.
        Then:
            It should be True whenever the call would bind, including
            for a ``**kwargs`` sink — being able to receive a value is
            the question, and no signature reveals what is done with it.
        """
        # Act & assert
        assert accepts_kwarg(build(), "flavor") is expected

    @pytest.mark.parametrize(
        ("tags", "expected"),
        [((), True), (("tag",), False)],
        ids=["no-positionals", "one-positional"],
    )
    def test_accepts_kwarg_should_depend_on_the_rest_of_the_call(self, tags, expected):
        """Test a positional-or-keyword parameter is call-dependent.

        Given:
            A callable declaring the parameter positional-or-keyword,
            and a caller that also forwards positional arguments.
        When:
            It is asked whether the keyword can be passed, once with a
            positional forwarded and once without.
        Then:
            It should differ between the two, because a forwarded
            positional binds the parameter first and the keyword then
            collides — so no rule keyed to the parameter's kind alone
            could be correct.
        """

        # Arrange
        def positional(flavor=None, *, colour: str = "") -> None: ...

        # Act & assert
        assert accepts_kwarg(positional, "flavor", *tags) is expected

    def test_accepts_kwarg_with_two_required_keywords(self):
        """Test one required keyword does not mask another.

        Given:
            A callable declaring two keyword-only parameters, neither
            with a default, and a caller asking about each in turn while
            supplying neither.
        When:
            The predicate is evaluated for both names.
        Then:
            It should be True for both. A probe that bound the call
            completely would fail on whichever parameter it omitted, so
            each keyword would be reported unusable because of the
            other, and a callable declaring both would be found to
            accept neither.
        """

        # Arrange
        def both(*tags: str, colour: str, flavor: str) -> None: ...

        # Act & assert
        assert accepts_kwarg(both, "flavor") is True
        assert accepts_kwarg(both, "colour") is True

    def test_accepts_kwarg_should_accept_a_partial_that_presupplies_the_value(self):
        """Test a pre-bound keyword remains passable.

        Given:
            A partial that pre-supplies the keyword.
        When:
            It is asked whether the keyword can be passed.
        Then:
            It should be True: the call binds and the caller's value
            overrides the pre-bound one, which is `functools.partial`'s
            documented composition rather than a special case.
        """
        # Arrange
        bound = functools.partial(_declaring, flavor="a")

        # Act & assert
        assert accepts_kwarg(bound, "flavor") is True

    def test_accepts_kwarg_should_accept_a_partial_binding_another_keyword(self):
        """Test pre-binding one keyword does not suppress another.

        Given:
            A callable declaring two keyword-only parameters, wrapped in
            a partial that pre-supplies only one of them.
        When:
            It is asked about the parameter the partial left unbound.
        Then:
            It should be True, so wrapping a callable to fix one value
            does not strip its declaration of the others.
        """

        # Arrange
        def two(*args: str, colour: str = "", flavor: str | None = None) -> None: ...

        bound = functools.partial(two, colour="red")

        # Act & assert
        assert accepts_kwarg(bound, "flavor") is True

    def test_accepts_kwarg_should_account_for_other_keywords_in_the_call(self):
        """Test an unrelated keyword in the call can make it unbindable.

        Given:
            A callable declaring the parameter but nothing else, and a
            caller that also intends to pass an unrelated keyword.
        When:
            It is asked whether the keyword can be passed as part of
            that whole call.
        Then:
            It should be False, since the question is about the call the
            caller will actually make rather than the parameter alone.
        """
        # Act & assert
        assert accepts_kwarg(_declaring, "flavor", colour="red") is False


class TestPresuppliesKwarg:
    """Test suite for `presupplies_kwarg`."""

    @pytest.mark.parametrize(
        ("build", "expected"),
        [
            (lambda: functools.partial(_declaring, flavor="a"), True),
            (lambda: functools.partial(_declaring, "tag"), False),
            (lambda: functools.partial(_declaring), False),
            (lambda: _declaring, False),
            (lambda: _Callable(), False),
            (lambda: min, False),
        ],
        ids=[
            "partial-binding-it",
            "partial-binding-a-positional",
            "partial-binding-nothing",
            "plain-function",
            "callable-object",
            "builtin",
        ],
    )
    def test_presupplies_kwarg_should_answer_whether_a_value_is_already_bound(
        self, build, expected
    ):
        """Test the predicate reports only values the callable has bound.

        Given:
            A callable that either binds the keyword through
            functools.partial or does not bind it at all.
        When:
            It is asked whether the keyword is already supplied.
        Then:
            It should answer True only for the partial that bound that
            keyword, so a caller can tell a decision already made from
            one still open to it.
        """
        # Act & assert
        assert presupplies_kwarg(build(), "flavor") is expected

    def test_presupplies_kwarg_should_ignore_a_declared_default(self):
        """Test a default is a fallback rather than a decision.

        Given:
            A callable declaring the keyword with a default, which
            `inspect.signature` renders identically to a partial's bound
            value.
        When:
            It is asked whether the keyword is already supplied.
        Then:
            It should answer False, since a default says what to use
            when nothing is passed rather than that the question is
            settled — the distinction the signature cannot express.
        """

        # Arrange
        def defaulted(*args: str, flavor: str = "a") -> None: ...

        # Act & assert
        assert presupplies_kwarg(defaulted, "flavor") is False

    def test_presupplies_kwarg_should_see_through_nested_partials(self):
        """Test a keyword bound by an inner partial is still reported.

        Given:
            A partial wrapping another partial, where the inner one
            bound the keyword.
        When:
            The outer partial is asked whether the keyword is supplied.
        Then:
            It should answer True, since functools.partial flattens when
            nested and a caller should not have to walk the chain.
        """
        # Arrange
        nested = functools.partial(functools.partial(_declaring, flavor="a"), "tag")

        # Act & assert
        assert presupplies_kwarg(nested, "flavor") is True


class TestRequiresKwarg:
    """Test suite for `requires_kwarg`."""

    @pytest.mark.parametrize(
        ("build", "expected"),
        [
            (lambda: lambda *args, flavor: None, True),
            (lambda: _declaring, False),
            (lambda: lambda *args, **kwargs: None, False),
            (lambda: min, False),
        ],
        ids=["required", "defaulted", "kwargs-sink", "builtin"],
    )
    def test_requires_kwarg_should_answer_whether_omitting_would_fail(
        self, build, expected
    ):
        """Test the predicate reports whether the keyword is mandatory.

        Given:
            A callable that requires the keyword, defaults it, absorbs
            it, or cannot be inspected.
        When:
            It is asked whether omitting the keyword would fail.
        Then:
            It should be True only for the callable that cannot be
            called without one, and False for the uninspectable case —
            deferring to the call rather than refusing on a guess.
        """
        # Act & assert
        assert requires_kwarg(build(), "flavor") is expected

    def test_requires_kwarg_with_an_unrelated_missing_parameter(self):
        """Test the answer is about the queried name and nothing else.

        Given:
            A callable requiring a keyword the caller does not supply,
            and which never names the keyword being asked about.
        When:
            The predicate is evaluated for the absent name.
        Then:
            It should be False. Treating any binding failure as evidence
            about the queried name reports a callable as requiring a
            parameter it does not declare, which sends a caller looking
            for the wrong argument.
        """

        # Arrange
        def elsewhere(*tags: str, colour: str) -> None: ...

        # Act & assert
        assert requires_kwarg(elsewhere, "flavor") is False

    def test_requires_kwarg_with_a_parameter_supplied_positionally(self):
        """Test an argument the caller already passes is not required.

        Given:
            A callable declaring the parameter positional-or-keyword
            with no default, and a caller forwarding one positional.
        When:
            The predicate is evaluated with that positional.
        Then:
            It should be False: the forwarded argument already fills the
            slot, so the caller need not name it again.
        """

        # Arrange
        def positional(flavor, *, colour: str = "") -> None: ...

        # Act & assert
        assert requires_kwarg(positional, "flavor", "x") is False
        assert requires_kwarg(positional, "flavor") is True

    def test_requires_kwarg_with_a_variadic_of_the_same_name(self):
        """Test a variadic parameter is never itself required.

        Given:
            Callables whose ``*args`` or ``**kwargs`` is spelled with
            the name being asked about.
        When:
            The predicate is evaluated for that name.
        Then:
            It should be False for both. A variadic absorbs rather than
            demands, so omitting it is always legal — and it carries no
            default, which is otherwise the marker for optional.
        """

        # Arrange
        def var_positional(*flavor: str) -> None: ...

        def var_keyword(**flavor: str) -> None: ...

        # Act & assert
        assert requires_kwarg(var_positional, "flavor") is False
        assert requires_kwarg(var_keyword, "flavor") is False

    def test_requires_kwarg_with_arguments_the_callable_rejects(self):
        """Test a caller's own bad arguments are not an answer.

        Given:
            A callable requiring the queried keyword, and a caller
            passing an argument the callable does not accept.
        When:
            The predicate is evaluated.
        Then:
            It should be False. The caller's arguments not fitting is a
            different fault from the queried name being mandatory, and
            reporting it as the latter would misdirect the caller.
        """

        # Arrange
        def mandatory(*tags: str, flavor: str) -> None: ...

        # Act & assert
        assert requires_kwarg(mandatory, "flavor", colour="red") is False

    def test_requires_kwarg_should_not_be_the_negation_of_accepts_kwarg(self):
        """Test the two predicates answer independent questions.

        Given:
            A callable that both accepts the keyword and cannot be
            called without one.
        When:
            Both predicates are evaluated against it.
        Then:
            Both should be True, since a caller that only sometimes
            holds a value needs to know it may pass and that it must.
        """

        # Arrange
        def mandatory(*args: str, flavor: str | None) -> None: ...

        # Act & assert
        assert accepts_kwarg(mandatory, "flavor") is True
        assert requires_kwarg(mandatory, "flavor") is True

    def test_requires_kwarg_should_ignore_a_presupplied_partial(self):
        """Test pre-binding satisfies the requirement.

        Given:
            A callable requiring the keyword, wrapped in a partial that
            pre-supplies it.
        When:
            It is asked whether omitting the keyword would fail.
        Then:
            It should be False: the partial already carries a value, so
            the caller is free to omit one.
        """

        # Arrange
        def mandatory(*args: str, flavor: str | None) -> None: ...

        bound = functools.partial(mandatory, flavor="a")

        # Act & assert
        assert requires_kwarg(bound, "flavor") is False


class TestUnbindableCall:
    """Test suite for `unbindable_call`."""

    def test_unbindable_call_with_a_satisfiable_call(self):
        """Test a call that binds reports nothing.

        Given:
            A callable and a set of arguments that satisfy it.
        When:
            The whole call is checked.
        Then:
            It should be None, the caller's signal to proceed.
        """

        # Arrange
        def factory(*tags: str, colour: str, flavor: str = "") -> None: ...

        # Act & assert
        assert unbindable_call(factory, "t", colour="red") is None

    def test_unbindable_call_with_a_missing_required_argument(self):
        """Test the failure names the argument at fault.

        Given:
            A callable requiring a keyword the caller does not supply.
        When:
            The whole call is checked.
        Then:
            It should return a message naming that argument, so a caller
            can report which parameter it failed to satisfy rather than
            leaving a TypeError to surface from a frame the user cannot
            see.
        """

        # Arrange
        def factory(*tags: str, colour: str) -> None: ...

        # Act
        reason = unbindable_call(factory, "t")

        # Assert
        assert reason is not None
        assert "colour" in reason

    def test_unbindable_call_with_an_unexpected_keyword(self):
        """Test an argument the callable does not accept is reported.

        Given:
            A caller passing a keyword the callable never declares.
        When:
            The whole call is checked.
        Then:
            It should return a message naming that keyword.
        """

        # Arrange
        def factory(*tags: str) -> None: ...

        # Act
        reason = unbindable_call(factory, "t", colour="red")

        # Assert
        assert reason is not None
        assert "colour" in reason

    def test_unbindable_call_with_an_uninspectable_callable(self):
        """Test an unreadable signature is not reported as a failure.

        Given:
            A built-in whose signature cannot be inspected.
        When:
            The whole call is checked.
        Then:
            It should be None. There is nothing to check against, and
            refusing a callable on a guess is the behaviour binding
            exists to avoid.
        """
        # Act & assert
        assert unbindable_call(min, "t", colour="red") is None
