import setuptools

# with open("README.md", "r") as fh:
#     long_description = fh.read()

setuptools.setup(
    name="src", # Replace with your own username
    
    version="0.0.1",
    author="Codema",
    author_email="rowan.molony@codema.ie",
    description="Source code for DREM",
    # long_description=long_description,
    long_description_content_type="text/markdown",
    # url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        # "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

# Source: https://packaging.python.org/tutorials/packaging-projects/#setup-py
