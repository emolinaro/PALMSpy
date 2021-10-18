import setuptools

from src.tools import Metadata

meta = Metadata()

with open("README.md", "r") as file:
    long_description = file.read()

setuptools.setup(
    name='palmspy',
    version=meta.version,
    scripts=['palmspy.py', 'palmspy'],
    py_modules=['src.__init__', 'src.help_menu', 'src.GPSProcessing', 'src.AccProcessing', 'src.tools'],
    author=meta.authors,
    author_email=meta.email,
    description=meta.description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/emolinaro/HABITUS",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: MIT",
        "Operating System :: OS Independent",
    ],
)
