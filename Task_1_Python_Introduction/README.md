# This app downloads info about rooms and students to mysql database and creates files with result of queries:

query1: The list of rooms and the number of students in each of them

query2: 5 rooms with the smallest average age of students

query3: 5 rooms with the biggest difference in the age of students

query4: List of rooms where students of different sexes live

# To run this app use command:

```sh
$ ./run.sh -r [path_rooms] -s [path_students] -f [format]

```

[path_rooms]  - path to file rooms.json

[path_students]  - path to file students.json

[format] - format of output file: json or xml
