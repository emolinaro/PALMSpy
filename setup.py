import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='habitus',
     version='1.0',
     scripts=['habitus.py','parser.py','gen_settings.py','GPSProcessing.py','AccProcessing.py','parser.py','tools.py'],
     author="Emiliano Molinaro",
     author_email="emil.molinaro@gmail.com",
     description="A program to detect personal activity patterns of individual participants wearing \
                  a GPS data logger and a physical activity monitor",
     long_description=long_description,
     long_description_content_type="text/markdown",
     url="https://github.com/emolinaro/HABITUS",
     packages=setuptools.find_packages(),
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: GPL-2",
         "Operating System :: OS Independent",
     ],
 )