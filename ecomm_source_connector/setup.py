from setuptools import setup, find_packages

setup(
    name='ecomm_source_connector',
    version='0.1.0',
    packages=find_packages(exclude=['build']),
    package_dir={"": "src"},
    include_package_data=True,  # This will enable MANIFEST.in to include files
    package_data={
        '': ['*.yaml'],  # Include all YAML files in the package
    },
    python_requires='>=3.6'
)