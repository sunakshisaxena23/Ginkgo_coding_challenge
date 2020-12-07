import setuptools
try:  # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:  # for pip <= 9.0.3
    from pip.req import parse_requirements

reqs = parse_requirements('requirements.txt', session=False)

try:
    requirements = [str(ir.req) for ir in reqs]
except:
    requirements = [str(ir.requirement) for ir in reqs]

setuptools.setup(
    install_requires=requirements,
    name='Ginkgo_coding_challenge',
    version='0.0.1',
    description='ETL from Oracle to redshift',
    author='sunakshi',
    author_email='sunakshi.saxena12@gmail.com',
    packages=['ginkgo_coding_challenge'],
    zip_safe=False
)

