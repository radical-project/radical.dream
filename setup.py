from setuptools import setup, find_packages

setup_args = {}

setup_args['name']                 = "hydraa"
setup_args['version']              = "1.0.0"
setup_args['scripts']              = ['hydraa/services/caas_manager/config/deploy_kuberentes_local.sh']
setup_args['packages']             = find_packages()
setup_args['package_data']         = {'': ['*.sh']}
setup_args['python_requires']      = '>=3.6'
setup_args['install_requires']     = ['boto3',
                                      'azure-core',
                                      'azure-batch', 
                                      'azure-common',
                                      'azure-identity',
                                      'azure-mgmt-core',
                                      'azure-mgmt-storage',
                                      'azure-mgmt-resource',
                                      'azure-mgmt-containerinstance']

setup(**setup_args)
