from setuptools import setup, find_packages

setup_args = {}

setup_args['name']                 = "hydraa"
setup_args['version']              = "1.0.0"
setup_args['packages']             = find_packages()
setup_args['python_requires']      = '>=3.6'
setup_args['install_requires']     = ['boto3']
setup_args['include_package_data'] = True

setup(**setup_args)