from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="map-tile-downloader",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A modular Python application for downloading map tiles with support for rate limiting and IP rotation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/map-tile-downloader",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=[
        'pyyaml>=6.0',
        'requests>=2.31.0',
        'minio>=7.1.16',
        'mbtiles>=0.5.1',
        'python-dotenv>=1.0.0',
        'schedule>=1.2.0',
        'python-slugify>=8.0.1',
    ],
    entry_points={
        'console_scripts': [
            'map-tile-downloader=map_downloader.__main__:main',
        ],
    },
)
