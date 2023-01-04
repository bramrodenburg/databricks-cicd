from setuptools import find_packages, setup
from typing import List


def parse_requirements(path: str) -> List[str]:
    with open(path) as f:
        requirements = f.read().splitlines()
        return list(filter(lambda r: r != "",
            requirements))


# Packages required for this package and notebooks
PACKAGE_REQUIREMENTS = parse_requirements("python_requirements.txt")


# Packages for local development and testing only.
LOCAL_REQUIREMENTS = parse_requirements("python_local_requirements.txt")


# Packages for development and testing
TEST_REQUIREMENTS = parse_requirements("python_test_requirements.txt")


setup(
    packages=find_packages(exclude=["test", "test.*"]),
    setup_requires=["setuptools", "wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"local": LOCAL_REQUIREMENTS, "test": TEST_REQUIREMENTS},
)

