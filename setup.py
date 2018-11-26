from setuptools import setup, find_packages
from os import path

from importlib.machinery import SourceFileLoader


module = SourceFileLoader(
    "version", path.join("aiocarbon", "version.py")
).load_module()


setup(
    name='aiocarbon',
    version=module.__version__,
    packages=find_packages(exclude=['tests', 'example', 'env']),
    license=module.package_license,
    description=module.package_info,
    long_description=open("README.rst").read(),
    url=module.project_home,
    author=module.__author__,
    author_email=module.team_email,
    provides=["aiocarbon"],
    python_requires=">3.5.*, <4",
    keywords=["aio", "python", "asyncio", "carbon", "graphite", "client"],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Natural Language :: Russian',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'Programming Language :: Cython',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries',
        'Topic :: System',
        'Topic :: System :: Operating System',
    ],
    install_requires=[
        "immutables",
    ]
)
