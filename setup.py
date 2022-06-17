import setuptools

requires = [
    'numpy >= 1.18.1',
    'redis >= 3.4.1',
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
        ],
    install_requires=requires,
    entry_points = {
        'console_scripts':[
            'automator = automator.cli:cli',
            'blproc_manual = automator.blproc_manual:cli', 
            ]
        },
    scripts = [
        'scripts/processing_example.sh'       
        ]

    )
