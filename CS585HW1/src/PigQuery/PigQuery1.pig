customers = LOAD '/Users/merqurius/Workspace/CS585/CS585HW1/Customer.txt' USING  PigStorage(',') AS  (id:int,name:chararray,age:int,gender:chararray,CountryCode:int,salary:float);
A = foreach customers generate id, name;
transactions = LOAD '/Users/merqurius/Workspace/CS585/CS585HW1/Transaction.txt' USING PigStorage(',') as (trans_id:int, cust_id:int, total:float, num_items:int,  description:chararray);
B = foreach transactions generate cust_id,trans_id; 
alldata = JOIN A BY id, B BY cust_id;
C = GROUP alldata by name;
D = foreach C  Generate group as name,COUNT(alldata.trans_id) as cnt;
E = Group D all;
F = foreach E Generate MIN(D.cnt) as min_val;
O = FILTER D BY cnt==(int)F.min_val;
STORE O INTO '/Users/merqurius/Workspace/CS585/CS585HW1/pig_Output/ ' USING PigStorage (',');