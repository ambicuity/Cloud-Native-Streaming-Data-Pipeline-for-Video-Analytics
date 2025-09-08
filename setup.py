from setuptools import setup, find_packages

setup(
    name="video-analytics-pipeline",
    version="1.0.0",
    description="Cloud-Native Streaming Data Pipeline for Video Analytics on GCP",
    author="Technology Professional",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "apache-beam[gcp]>=2.52.0",
        "google-cloud-pubsub>=2.18.0",
        "google-cloud-monitoring>=2.16.0",
        "google-cloud-logging>=3.8.0",
        "google-cloud-storage>=2.10.0",
        "pandas>=2.1.0",
        "jsonschema>=4.19.0",
        "pydantic>=2.5.0",
        "python-dotenv>=1.0.0",
        "click>=8.1.0",
        "pyyaml>=6.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.11.0",
            "flake8>=6.1.0",
            "mypy>=1.7.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "video-pipeline=video_analytics_pipeline.main:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)