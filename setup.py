import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="rdbms-op",
    version="0.0.6",
    author="Yejin Jo",
    author_email="ojjy90@gmail.com",
    description="Operational Helper for Relational Database Managemant System",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ojjy/rdbms-op",
    project_urls={
        "Bug Tracker": "https://github.com/ojjy/rdbms-op/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)