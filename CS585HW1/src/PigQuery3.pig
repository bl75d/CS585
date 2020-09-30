customers = LOAD '/Users/merqurius/Workspace/CS585/CS585HW1/Customer.txt' USING  PigStorage(',') AS  (id:int,name:chararray,age:int,gender:chararray,CountryCode:int,salary:float);
A = foreach customers generate id, CountryCode;
B = GROUP A by CountryCode;
C = foreach B Generate group as cc, COUNT(A.id) as cnt;
O = FILTER C BY (cnt<2000) OR (cnt>5000);
STORE O INTO '/Users/merqurius/Workspace/CS585/CS585HW1/pig_Output/ ' USING PigStorage (',');

