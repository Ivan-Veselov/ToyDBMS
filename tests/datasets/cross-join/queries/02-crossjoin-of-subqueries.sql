select * from (select * from A, B where A.id = B.aid;) as t1, (select * from C, D where C.id = D.cid;) as t2;
