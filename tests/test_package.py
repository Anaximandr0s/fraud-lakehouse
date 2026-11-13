import fraud_lakehouse


def test_package_is_installed_from_src() -> None:
    # Guards the src/ layout: tests must import the installed package, not a stray copy.
    assert "/src/fraud_lakehouse/" in fraud_lakehouse.__file__
