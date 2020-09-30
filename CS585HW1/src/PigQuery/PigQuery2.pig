customers = LOAD '/Users/merqurius/Workspace/CS585/CS585HW1/Customer.txt' USING  PigStorage(',') AS  (id:int,name:chararray,age:int,gender:chararray,CountryCode:int,salary:float);
A = foreach customers generate id, name, salary;
transactions = LOAD '/Users/merqurius/Workspace/CS585/CS585HW1/Transaction.txt' USING PigStorage(',') as (trans_id:int, cust_id:int, total:float, num_items:int,  description:chararray);
B = foreach transactions generate cust_id,total,num_items; 
C = GROUP B by cust_id;
D = foreach C Generate group as cust_id,COUNT(B.total) as num_trans,SUM(B.total) as totalsum, MIN(B.num_items) as minitems;
E = Join A by id, D by cust_id USING 'replicated';
O = foreach E Generate A::id as c_id, A::name as c_name, A::salary as c_salary,D::num_trans as c_num_trans, D::totalsum as c_totalsum, D::minitems as c_minitems;
STORE O INTO '/Users/merqurius/Workspace/CS585/CS585HW1/pig_Output/ ' USING PigStorage (',');

