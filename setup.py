from setuptools import setup, find_packages

setup(
    name="xflow",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas>=1.5.0",
        "numpy>=1.23.0",
        "websockets>=10.0",
        "python-binance>=1.0.15",
        "httpx>=0.23.0",
    ],
    python_requires=">=3.8",
    
    url="https://github.com/ArapKBett/Xflow",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Traders",
        "Topic :: Software Development :: Libraries",
    ],
)
