from setuptools import setup, find_packages

setup_args = {}

setup_args['name']                 = "hydraa"
setup_args['version']              = "1.0.0"
setup_args['scripts']              = ['hydraa/services/caas_manager/config/bootstrap_kubernetes.sh',
                                      'hydraa/services/caas_manager/config/kubeflow_kubernetes.yaml']
setup_args['packages']             = find_packages()
setup_args['package_data']         = {'': ['*.sh']}
setup_args['python_requires']      = '>=3.6'
setup_args['install_requires']     = ['boto3',
                                      'pandas',
                                      'azure.cli<=2.42.0',
                                      'kubernetes',
                                      'azure-core<=2.42.0',
                                      'python-chi',
                                      'azure-common',
                                      'azure-identity',
                                      'azure-mgmt-core',
                                      'azure-mgmt-storage',
                                      'azure-mgmt-resource',
                                      'python-openstackclient',
                                      'azure-mgmt-containerinstance']

setup(**setup_args)
