# duplicatesFinder

**Author**: Alessio Russo (russo.alessio@outlook.com)
**Description**: Given a folder the app finds files of the same content across the subdirectories of that folder.
**How to use**: Launch the application passing as argument the parent folder, e.g:

''sh
python listDuplicates.py ../
''

Use the option -t to enable threading. 

Use the option -i __file__ to specify a file that includes a list of directories (absolute path) that have to be ignored during the analysis.

Example:

''sh
python listduplicates.py ../../. -t -i ignoreDirectories.txt
''
