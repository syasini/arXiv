from setuptools import setup

with open("requirements.txt", "r") as f:
    reqs = [line.rstrip("\n") for line in f if line != "\n"]

setup(
    name='arxivester',
    version='0.3',
    packages=['arxivester'],
    install_requires=reqs,
    url='https://github.com/syasini/arXiv',
    license='',
    author='Siavash Yasini, Amin Oji',
    author_email='siavash.yasini@gmail.com',
    description='a python code for scraping arXiv and inSPIRE-HEP.'
    )

