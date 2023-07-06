import setuptools

requires = [
    'numpy >= 1.18.1',
    'redis >= 3.4.1',
    'requests == 2.28.1',
    'katsdptelstate >= 0.11',
    'PyYAML >= 6.0',
    'pyzmq >= 25.0.0'
    ]

setuptools.setup(
    name = 'coordinator',
    version = '2.0',
    url = 'https://github.com/UCBerkeleySETI/commensal-automator',
    license = 'MIT',
    author = 'Daniel Czech, Dave MacMahon, Kevin Lacker',
    author_email = 'danielc@berkeley.edu',
    description = 'Automation for Breakthrough Listen\'s commensal observing',
    packages = ['coordinator'],
    install_requires=requires,
    entry_points = {
        'console_scripts':[
            'coordinator = coordinator.cli:cli',
            'bluse_analyzer = coordinator.bluse_analyzer:cli'
            ]
        },
    )
