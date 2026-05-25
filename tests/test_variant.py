from rowsmyth._variant import variant


def test_variant_marks_function():
    @variant
    def premium(cls):
        return {cls: "premium"}

    assert getattr(premium, "_is_variant", False) is True


def test_variant_preserves_return_value():
    @variant
    def admin(cls):
        return {"name": "admin"}

    assert admin("ignored_cls") == {"name": "admin"}
