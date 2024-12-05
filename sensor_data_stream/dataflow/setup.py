import setuptools

REQUIRED_PACKAGES=[
    'apache_beam',
    'xmltodict',
    'google-cloud-bigquery',
    'google-cloud-storage',
    'google-cloud-pubsub',
]

setuptools.setup(
    name='install_package',
    version='0.0.1',
    description='installing Workflow package',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    include_package_data=True
)
