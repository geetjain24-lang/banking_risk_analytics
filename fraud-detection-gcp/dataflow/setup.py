"""
Setup file required by Dataflow to install dependencies on worker machines.
"""

from setuptools import setup, find_packages

setup(
    name="fraud-detection-pipeline",
    version="1.0",
    packages=find_packages(),
)