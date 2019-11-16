import setuptools

from src.tools import Metadata

meta = Metadata()

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='habitus',
     version=meta.version,
     scripts=['habitus.py','habitus'],
     py_modules=['src.__init__','src.help_menu','src.GPSProcessing','src.AccProcessing','src.tools'],
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