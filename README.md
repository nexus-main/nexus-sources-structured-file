# Nexus.Sources.StructureFile

This data source extension makes a base class available to simplify reading structured data files into Nexus.

## Test Case Description

These tests cover all cases for file/folder structures which contain files with a maximum length. This means they are allowed to be shorter than the upper limit but must not exceed it. The limit must be part of the source configuration because Nexus does not know how long a file is.

| Case | Description                                                                                               | UTC offset         | File period          | Regex |
| ---- | --------------------------------------------------------------------------------------------------------- | ------------------ | -------------------- | ----- |
| A    | text folder, month folder, day folder, datetime file, ignore unrelated folders                            | 00:00:00           | 00:10:00             |       |
| B    | month folder, text folder, day folder, datetime file                                                      | 00:00:00           | 00:10:00             |       |
| C    | day folder, datetime file                                                                                 | 00:00:00           | 00:10:00             |       |
| D    | text folder, datetime file, separate file source groups for seperate folders                              | 02:00:00, 01:00:00 | 00:10:00, 1.00:00:00 |       |
| E    | month folder, datetime file, separate file source groups for common folder                                | 00:00:00           | 1.00:00:00, 00:30:00 |       |
| F    | month folder, date + hour + random number file                                                            | 00:00:00           | 01:00:00             | x     |
| G    | day folder with prefix, time file                                                                         | 00:00:00           | 00:00:01             |       |
| H    | datetime file (no parent folders)                                                                         | 00:00:00           | 00:10:00             |       |
| I    | datetime folder, text file                                                                                | 00:00:00           | 00:05:00             |       |
| J    | datetime file with varying format and changing root folder, multiple time-limited files sources per group | 00:00:00           | 1.00:00:00           |       |
| K    | datetime file (no parent folders)                                                                         | 00:00:00           | 1.00:00:00           |       |
| L    | datetime file with version suffix                                                                         | 00:00:00           | 1.00:00:00           | x     |
| M    | datetime file with random start time                                                                      | 00:00:00           | 01:00:00             | x     |
| N    | date folder, time file                                                                                    | 12:00:00           | 06:00:00             |       |
| O    | datetime file with random start time and interval exceeding end                                           | 00:00:00           | 01:00:00             | x     |
| P    | datetime file with file name offset                                                                       | 00:00:00           | 00:10:00             |       |
