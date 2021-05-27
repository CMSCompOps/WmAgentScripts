from setuptools import setup, find_packages
import os
import sys

setup(name='wmagentscripts',
      author="Hasan Ozturk",
      version='0.0.1',
      description="WMAgentScripts",
      python_requires=">=3.6",
      package_dir={"": "src/python"},
      packages= find_packages(where="src/python")
)
