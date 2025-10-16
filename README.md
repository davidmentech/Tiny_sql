# Tiny_sql
Hello in this project i tried to implement DB that supports CREATE(for tables),SELECT,INSERT,DELETE with string(unlimitied in length) and int as column types.  
in this project you will also find template B+ tree implemntation (in the file BPlusTree.h) which saves the values to file in the filesystem (each B+ tree will generate 2 files).  
The B+ tree implementation uses conversion functions (which are at the head of the file), so in order for the tree to work with custom new types you need to add the converstion from the custom type to string and from string to the custom type.  
forbbiden keys in any command: space (" "),comma (","),seperator ("|")!!!!  
the DB support following commands:  
CREATE table_name column_name_1:type ... column_name_n:type KEY column_name_1 ... column_name_k  
  each word must be sperated from the next with space  
  2 tables with the same name is not allowed  
  2 columns with the same name is not allowed  
  type will be only S (for string) and I (for int)   
  the key must the first k (k to your chosing 1 or up) columns specified  
INSERT val1 ... valn TO table_name  
  vals must be the same type like the columns (string represnted by "val" and int by val)  
  table must be create first  
  number of values must be the same as column number spceified  
DELETE val1 ... valk FROM table_name  
  value list is key value which was enterd when insert  
  table must be created and record must be inserted before  
SELECT * FROM table_name  
  prints all values from table  
SELECT column_name_1 ... column_name_t from table  
  prints value from columns specified with same order spcified (meaning key can be printed at the end of the table)  
can also add WHERE clause , supported ops: >= , <= , !=, ==. if the clause is on the key you better(for better preformence) use KEY>=val1,val2,...,valk
clause must be with no spaces and only with commas if the key is bigger then one column (can only have clause with one column if not using key)  
there is also GC command when the system gets slow or the size of files is getting to big and EXIT when done (will save all the data from before)  
the system can also restore the last state of the system (prompt will be shown at start)

  
