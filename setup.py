import setuptools

requires = [
    'numpy >= 1.18.1',
    'redis >= 3.4.1',
    'katsdptelstate >= 0.11'
    ]

setuptools.setup(
    name = 'automator',
    version = '1.0',
    url = 'https://github.com/UCBerkeleySETI/commensal-automator',
    license = 'MIT',
    author = 'Daniel Czech',
    author_email = 'danielc@berkeley.edu',
    description = 'Automation for Breakthrough Listen\'s commensal observing',
    packages = [
        'automator',
        'coordinator',
        ],
    install_requires=requires,
    entry_points = {
        'console_scripts':[
            'automator = automator.cli:cli',
            'coordinator = coordinator.coordinator_start:cli', 
            ]
        },

    )
