# Nexus.Sources.StructureFile

This data source extension makes a base class available to simplify reading structured data files into Nexus.

## Test Case Description

| Case | Description                                                                                               | UTC offset         | File period          |
| ---- | --------------------------------------------------------------------------------------------------------- | ------------------ | -------------------- |
| A    | text folder, month folder, day folder, datetime file, ignore unrelated folders                            | 00:00:00           | 00:10:00             |
| B    | month folder, text folder, day folder, datetime file                                                      | 00:00:00           | 00:10:00             |
| C    | day folder, datetime file                                                                                 | 00:00:00           | 00:10:00             |
| D    | text folder, datetime file, separate file source groups for seperate folders                              | 02:00:00, 01:00:00 | 00:10:00, 1.00:00:00 |
| E    | month folder, datetime file, separate file source groups for common folder                                | 00:00:00           | 1.00:00:00, 00:30:00 |
| F    | month folder, date + hour + random number file, file template + preselector + selector                    | 00:00:00           | 01:00:00             |
| G    | day folder with prefix, time file                                                                         | 00:00:00           | 00:00:01             |
| H    | datetime file (no parent folders)                                                                         | 00:00:00           | 00:10:00             |
| I    | datetime folder, text file                                                                                | 00:00:00           | 00:05:00             |
| J    | datetime file with varying format and changing root folder, multiple time-limited files sources per group | 00:00:00           | 1.00:00:00           |
| K    | datetime file (no parent folders)                                                                         | 00:00:00           | 1.00:00:00           |
| L    | datetime file with version suffix, file template + preselector + selector                                 | 00:00:00           | 1.00:00:00           |
| M    | datetime file with random start time, file template + preselector + selector                              | 00:00:00           | 01:00:00             |
| N    | date folder, time file                                                                                    | 12:00:00           | 06:00:00             |
