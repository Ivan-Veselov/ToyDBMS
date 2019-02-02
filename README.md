# Toy DBMS

Simple  in-memory  database  management  system that was written during *Advanced Databases* course in Higher School of Economics (Autumn 2018). System supports common SQL query operations (joins, projections, subqueries, filters, unique operator) and performs a few optimization steps to speed up query execution.

Parser and architectural code was written by course organizers.

## Building

Requirements:
- C++14 compiler
- make
- bison 3.0.4
- flex 2.6.1
- patch
- readline

`make` command runs building.

## Running

File `testexe` accepts SQL query from stdin and executes it. Each table must reside in its separare `csv` file in `tables` folder. The format is the following: 

```
inum,stext
15,test
1,two words
3,just another test
```

If you know Russian you may check file `README-RUS.md` which contains more comprehensive description.