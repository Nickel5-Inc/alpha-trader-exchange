# setup.py

from setuptools import setup, find_packages

setup(
    name="alphatrade",  # Changed from bittensor_subnet_template to alphatrade
    version="1.0.0",
    description="Alpha Trade Exchange Subnet",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'aiosqlite>=0.19.0',
        'pandas>=2.0.0',
        'aiohttp>=3.9.0',
        'psutil>=5.9.0',
        'pytest>=8.0.0',
        'pytest-asyncio>=0.25.0',
        'pytest-cov>=4.1.0',
        'black>=24.0.0',
        'isort>=5.13.0',
        'flake8>=7.0.0'
    ]
)
