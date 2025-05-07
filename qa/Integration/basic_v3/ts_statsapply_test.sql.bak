set enable_multimodel=off;
set cluster setting ts.sql.query_opt_mode = 1110;
create ts database tpch;
use tpch;
set cluster setting sql.stats.ts_automatic_collection.enabled = false;
-- REGION TABLE
CREATE TABLE REGION (
                        k_timestamp timestamp not null,
                        R_REGIONKEY INTEGER NOT NULL,
                        R_NAME CHAR(25) NOT NULL,
                        R_COMMENT VARCHAR(152),
                        R_NULL VARCHAR(10)
)tags (size int not null) primary tags (size);
-- NATION TABLE
CREATE TABLE NATION (
                        k_timestamp timestamp not null,
                        N_NATIONKEY INTEGER NOT NULL,
                        N_NAME CHAR(25) NOT NULL,
                        N_REGIONKEY INTEGER NOT NULL,
                        N_COMMENT VARCHAR(152),
                        N_NULL VARCHAR(10)
)tags (size int not null) primary tags (size) ;
-- SUPPLIER TABLE
CREATE TABLE SUPPLIER (
                          k_timestamp timestamp not null,
                          S_SUPPKEY INTEGER NOT NULL,
                          S_NAME CHAR(25) NOT NULL,
                          S_ADDRESS VARCHAR(40) NOT NULL,
                          S_NATIONKEY INTEGER NOT NULL,
                          S_PHONE CHAR(15) NOT NULL,
                          S_ACCTBAL float8 NOT NULL,
                          S_COMMENT VARCHAR(101) NOT NULL,
                          S_NULL VARCHAR(10)
)tags (size int not null) primary tags (size);
-- PART TABLE
CREATE TABLE PART (
                      k_timestamp timestamp not null,
                      P_PARTKEY INTEGER NOT NULL,
                      P_NAME VARCHAR(55) NOT NULL,
                      P_MFGR CHAR(25) NOT NULL,
                      P_BRAND CHAR(10) NOT NULL,
                      P_TYPE VARCHAR(25) NOT NULL,
                      P_SIZE INTEGER NOT NULL,
                      P_CONTAINER CHAR(10) NOT NULL,
                      P_RETAILPRICE float8 NOT NULL,
                      P_COMMENT VARCHAR(23) NOT NULL,
                      P_NULL VARCHAR(10)
)tags (size int not null) primary tags (size);
-- PARTSUPP TABLE
CREATE TABLE PARTSUPP (
                          k_timestamp timestamp not null,
                          PS_PARTKEY INTEGER NOT NULL,
                          PS_SUPPKEY INTEGER NOT NULL,
                          PS_AVAILQTY INTEGER NOT NULL,
                          PS_SUPPLYCOST float8 NOT NULL,
                          PS_COMMENT VARCHAR(199) NOT NULL,
                          PS_NULL VARCHAR(10)
)tags (size int not null) primary tags (size);
-- CUSTOMER TABLE
CREATE TABLE CUSTOMER (
                          k_timestamp timestamp not null,
                          C_CUSTKEY INTEGER NOT NULL,
                          C_NAME VARCHAR(25) NOT NULL,
                          C_ADDRESS VARCHAR(40) NOT NULL,
                          C_NATIONKEY INTEGER NOT NULL,
                          C_PHONE CHAR(15) NOT NULL,
                          C_ACCTBAL float8 NOT NULL,
                          C_MKTSEGMENT CHAR(10) NOT NULL,
                          C_COMMENT VARCHAR(117) NOT NULL,
                          C_NULL VARCHAR(10)
)tags (size int not null) primary tags (size);
-- ORDERS TABLE
CREATE TABLE ORDERS (
                        k_timestamp timestamp not null,
                        O_ORDERKEY INTEGER NOT NULL,
                        O_CUSTKEY INTEGER NOT NULL,
                        O_ORDERSTATUS CHAR(1) NOT NULL,
                        O_TOTALPRICE float8 NOT NULL,
                        O_ORDERDATE timestamp NOT NULL,
                        O_ORDERPRIORITY CHAR(15) NOT NULL,
                        O_CLERK CHAR(15) NOT NULL,
                        O_SHIPPRIORITY INTEGER NOT NULL,
                        O_COMMENT VARCHAR(79) NOT NULL,
                        O_NULL VARCHAR(10)
)tags (size int not null) primary tags (size);
-- LINEITEM TABLE
CREATE TABLE LINEITEM (
                          k_timestamp timestamp not null,
                          L_ORDERKEY INTEGER NOT NULL,
                          L_PARTKEY INTEGER NOT NULL,
                          L_SUPPKEY INTEGER NOT NULL,
                          L_LINENUMBER INTEGER NOT NULL,
                          L_QUANTITY float8 NOT NULL,
                          L_EXTENDEDPRICE float8 NOT NULL,
                          L_DISCOUNT float8 NOT NULL,
                          L_TAX float8 NOT NULL,
                          L_RETURNFLAG CHAR(1) NOT NULL,
                          L_LINESTATUS CHAR(1) NOT NULL,
                          L_SHIPDATE timestamp NOT NULL,
                          L_COMMITDATE timestamp NOT NULL,
                          L_RECEIPTDATE timestamp NOT NULL,
                          L_SHIPINSTRUCT CHAR(25) NOT NULL,
                          L_SHIPMODE CHAR(10) NOT NULL,
                          L_COMMENT VARCHAR(44) NOT NULL,
                          L_NULL VARCHAR(10)
)tags (size int not null) primary tags (size);

-- no stats
-- Q1 price statistics report
explain explain select
                    l_returnflag,
                    l_linestatus,
                    sum(l_quantity) as sum_qty,
                    sum(l_extendedprice) as sum_base_price,
                    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                    avg(l_quantity) as avg_qty,
                    avg(l_extendedprice) as avg_price,
                    avg(l_discount) as avg_disc,
                    count(*) as count_order
                from
                    lineitem
                where
                        l_shipdate <= date '1998-12-01' - interval '79' day
                group by
                    l_returnflag,
                    l_linestatus
                order by
                    l_returnflag,
                    l_linestatus limit 1;
--Q2: minimum cost supplier query
explain select
            s_acctbal,
            s_name,
            n_name,
            p_partkey,
            p_mfgr,
            s_address,
            s_phone,
            s_comment
        from
            part,
            supplier,
            partsupp,
            nation,
            region
        where
                p_partkey = ps_partkey
          and s_suppkey = ps_suppkey
          and p_size = 12
          and p_type like '%NICKEL'
          and s_nationkey = n_nationkey
          and n_regionkey = r_regionkey
          and r_name = 'MIDDLE EAST'
          and ps_supplycost = (
            select
                min(ps_supplycost)
            from
                partsupp,
                supplier,
                nation,
                region
            where
                    p_partkey = ps_partkey
              and s_suppkey = ps_suppkey
              and s_nationkey = n_nationkey
              and n_regionkey = r_regionkey
              and r_name = 'MIDDLE EAST'
        )
        order by
            s_acctbal desc,
            n_name,
            s_name,
            p_partkey limit 100;
--Q3: shipping priority query
explain select
            l_orderkey,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            o_orderdate,
            o_shippriority
        from
            customer,
            orders,
            lineitem
        where
                c_mktsegment = 'AUTOMOBILE'
          and c_custkey = o_custkey
          and l_orderkey = o_orderkey
          and o_orderdate < date '1995-03-27'
          and l_shipdate > date '1995-03-27'
        group by
            l_orderkey,
            o_orderdate,
            o_shippriority
        order by
            revenue desc,
            o_orderdate limit 10;
--Q4: order priority query
explain select
            o_orderpriority,
            count(*) as order_count
        from
            orders
        where
                o_orderdate >= date '1997-01-01'
          and o_orderdate < date '1997-01-01' + interval '3' month
          and exists (
            select
                *
            from
                lineitem
            where
                    l_orderkey = o_orderkey
              and l_commitdate < l_receiptdate
        )
        group by
            o_orderpriority
        order by
            o_orderpriority limit 1;
--Q5: query the revenue that a supplier brings to the company in a certain region
explain select
            n_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue
        from
            customer,
            orders,
            lineitem,
            supplier,
            nation,
            region
        where
                c_custkey = o_custkey
          and l_orderkey = o_orderkey
          and l_suppkey = s_suppkey
          and c_nationkey = s_nationkey
          and s_nationkey = n_nationkey
          and n_regionkey = r_regionkey
          and r_name = 'MIDDLE EAST'
          and o_orderdate >= date '1994-01-01'
          and o_orderdate < date '1994-01-01' + interval '1' year
        group by
            n_name
        order by
            revenue desc limit 1;
--Q6: forecast revenue change query
explain select
            sum(l_extendedprice * l_discount) as revenue
        from
            lineitem
        where
                l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
          and l_discount between 0.04 - 0.01 and 0.04 + 0.01
          and l_quantity < 24 limit 1;
--Q7: freight Profitability query
explain select
            supp_nation,
            cust_nation,
            l_year,
            sum(volume) as revenue
        from
            (
                select
                    n1.n_name as supp_nation,
                    n2.n_name as cust_nation,
                    extract(year from l_shipdate) as l_year,
                    l_extendedprice * (1 - l_discount) as volume
                from
                    supplier,
                    lineitem,
                    orders,
                    customer,
                    nation n1,
                    nation n2
                where
                        s_suppkey = l_suppkey
                  and o_orderkey = l_orderkey
                  and c_custkey = o_custkey
                  and s_nationkey = n1.n_nationkey
                  and c_nationkey = n2.n_nationkey
                  and (
                        (n1.n_name = 'FRANCE' and n2.n_name = 'CANADA')
                        or (n1.n_name = 'CANADA' and n2.n_name = 'FRANCE')
                    )
                  and l_shipdate between date '1995-01-01' and date '1996-12-31'
            ) as shipping
        group by
            supp_nation,
            cust_nation,
            l_year
        order by
            supp_nation,
            cust_nation,
            l_year limit 1;
--Q8: country market share Query
explain select
            o_year,
            sum(case
                    when nation = 'CANADA' then volume
                    else 0
                end) / sum(volume) as mkt_share
        from
            (
                select
                    extract(year from o_orderdate) as o_year,
                    l_extendedprice * (1 - l_discount) as volume,
                    n2.n_name as nation
                from
                    part,
                    supplier,
                    lineitem,
                    orders,
                    customer,
                    nation n1,
                    nation n2,
                    region
                where
                        p_partkey = l_partkey
                  and s_suppkey = l_suppkey
                  and l_orderkey = o_orderkey
                  and o_custkey = c_custkey
                  and c_nationkey = n1.n_nationkey
                  and n1.n_regionkey = r_regionkey
                  and r_name = 'AMERICA'
                  and s_nationkey = n2.n_nationkey
                  and o_orderdate between date '1995-01-01' and date '1996-12-31'
                  and p_type = 'SMALL POLISHED STEEL'
            ) as all_nations
        group by
            o_year
        order by
            o_year limit 1;
--Q9: product type profit estimation Query
explain select
            nation,
            o_year,
            sum(amount) as sum_profit
        from
            (
                select
                    n_name as nation,
                    extract(year from o_orderdate) as o_year,
                    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                from
                    part,
                    supplier,
                    lineitem,
                    partsupp,
                    orders,
                    nation
                where
                        s_suppkey = l_suppkey
                  and ps_suppkey = l_suppkey
                  and ps_partkey = l_partkey
                  and p_partkey = l_partkey
                  and o_orderkey = l_orderkey
                  and s_nationkey = n_nationkey
                  and p_name like '%firebrick%'
            ) as profit
        group by
            nation,
            o_year
        order by
            nation,
            o_year desc limit 1;
--Q10: query of freight problems
explain select
            c_custkey,
            c_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            c_acctbal,
            n_name,
            c_address,
            c_phone,
            c_comment
        from
            customer,
            orders,
            lineitem,
            nation
        where
                c_custkey = o_custkey
          and l_orderkey = o_orderkey
          and o_orderdate >= date '1993-04-01'
          and o_orderdate < date '1993-04-01' + interval '3' month
          and l_returnflag = 'R'
          and c_nationkey = n_nationkey
        group by
            c_custkey,
            c_name,
            c_acctbal,
            c_phone,
            n_name,
            c_address,
            c_comment
        order by
            revenue desc limit 20;
--Q11: inventory value query
explain select
            ps_partkey,
            sum(ps_supplycost * ps_availqty) as value
        from
            partsupp,
            supplier,
            nation
        where
            ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'ETHIOPIA'
        group by
            ps_partkey having
            sum(ps_supplycost * ps_availqty) > (
            select
            sum(ps_supplycost * ps_availqty) * 0.0001000000
            from
            partsupp,
            supplier,
            nation
            where
            ps_suppkey = s_suppkey
                          and s_nationkey = n_nationkey
                          and n_name = 'FRANCE'
            )
        order by
            value desc limit 1;
--Q12: shipping mode and order priority query
explain select
            l_shipmode,
            sum(case
                    when o_orderpriority = '1-URGENT'
                        or o_orderpriority = '2-HIGH'
                        then 1
                    else 0
                end) as high_line_count,
            sum(case
                    when o_orderpriority <> '1-URGENT'
                        and o_orderpriority <> '2-HIGH'
                        then 1
                    else 0
                end) as low_line_count
        from
            orders,
            lineitem
        where
                o_orderkey = l_orderkey
          and l_shipmode in ('FOB', 'TRUCK')
          and l_commitdate < l_receiptdate
          and l_shipdate < l_commitdate
          and l_receiptdate >= date '1997-01-01'
          and l_receiptdate < date '1997-01-01' + interval '1' year
        group by
            l_shipmode
        order by
            l_shipmode limit 1;
--Q13: consumer order quantity query
explain select
            c_count,
            count(*) as custdist
        from
            (
                select
                    c_custkey,
                    count(o_orderkey)
                from
                    customer left outer join orders on
                                c_custkey = o_custkey
                            and o_comment not like '%pending%deposits%'
                group by
                    c_custkey
            ) as c_orders (c_custkey, c_count)
        group by
            c_count
        order by
            custdist desc,
            c_count desc limit 1;
--Q14: promotion effect query
explain select
                    100.00 * sum(case
                                     when p_type like 'PROMO%'
                                         then l_extendedprice * (1 - l_discount)
                                     else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
        from
            lineitem,
            part
        where
                l_partkey = p_partkey
          and l_shipdate >= date '1997-01-01'
          and l_shipdate < date '1997-01-01' + interval '1' month limit 1;
--Q16: parts/supplier relationship query
explain select
            p_brand,
            p_type,
            p_size,
            count(distinct ps_suppkey) as supplier_cnt
        from
            partsupp,
            part
        where
                p_partkey = ps_partkey
          and p_brand <> 'Brand#53'
          and p_type not like 'SMALL PLATED%'
          and p_size in (13, 46, 9, 35, 22, 2, 24, 31)
          and ps_suppkey not in (
            select
                s_suppkey
            from
                supplier
            where
                    s_comment like '%Customer%Complaints%'
        )
        group by
            p_brand,
            p_type,
            p_size
        order by
            supplier_cnt desc,
            p_brand,
            p_type,
            p_size limit 1;
--Q17: small order revenue query
explain select
                sum(l_extendedprice) / 7.0 as avg_yearly
        from
            lineitem,
            part
        where
                p_partkey = l_partkey
          and p_brand = 'Brand#32'
          and p_container = 'LG BAG'
          and l_quantity < (
            select
                    0.2 * avg(l_quantity)
            from
                lineitem
            where
                    l_partkey = p_partkey
        ) limit 1;
--Q18: large order customer query
explain select
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice,
            sum(l_quantity)
        from
            customer,
            orders,
            lineitem
        where
                o_orderkey in (
                select
                    l_orderkey
                from
                    lineitem
                group by
                    l_orderkey having
                        sum(l_quantity) > 313
            )
          and c_custkey = o_custkey
          and o_orderkey = l_orderkey
        group by
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice
        order by
            o_totalprice desc,
            o_orderdate limit 100;
--Q19: discount income query
explain select
            sum(l_extendedprice* (1 - l_discount)) as revenue
        from
            lineitem,
            part
        where
            (
                        p_partkey = l_partkey
                    and p_brand = 'Brand#25'
                    and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    and l_quantity >= 5 and l_quantity <= 5 + 10
                    and p_size between 1 and 5
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                )
           or
            (
                        p_partkey = l_partkey
                    and p_brand = 'Brand#14'
                    and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    and l_quantity >= 16 and l_quantity <= 16 + 10
                    and p_size between 1 and 10
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                )
           or
            (
                        p_partkey = l_partkey
                    and p_brand = 'Brand#25'
                    and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    and l_quantity >= 28 and l_quantity <= 28 + 10
                    and p_size between 1 and 15
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                ) limit 1;
--Q20: supplier competitiveness query
explain select
            s_name,
            s_address
        from
            supplier,
            nation
        where
                s_suppkey in (
                select
                    ps_suppkey
                from
                    partsupp
                where
                        ps_partkey in (
                        select
                            p_partkey
                        from
                            part
                        where
                                p_name like 'steel%'
                    )
                  and ps_availqty > (
                    select
                            0.5 * sum(l_quantity)
                    from
                        lineitem
                    where
                            l_partkey = ps_partkey
                      and l_suppkey = ps_suppkey
                      and l_shipdate >= date '1993-01-01'
                      and l_shipdate < date '1993-01-01' + interval '1' year
                )
            )
          and s_nationkey = n_nationkey
          and n_name = 'MOZAMBIQUE'
        order by
            s_name limit 1;
--Q21: unable to deliver on time supplier query
explain select
            s_name,
            count(*) as numwait
        from
            supplier,
            lineitem l1,
            orders,
            nation
        where
                s_suppkey = l1.l_suppkey
          and o_orderkey = l1.l_orderkey
          and o_orderstatus = 'F'
          and l1.l_receiptdate > l1.l_commitdate
          and exists (
            select
                *
            from
                lineitem l2
            where
                    l2.l_orderkey = l1.l_orderkey
              and l2.l_suppkey <> l1.l_suppkey
        )
          and not exists (
            select
                *
            from
                lineitem l3
            where
                    l3.l_orderkey = l1.l_orderkey
              and l3.l_suppkey <> l1.l_suppkey
              and l3.l_receiptdate > l3.l_commitdate
        )
          and s_nationkey = n_nationkey
          and n_name = 'CANADA'
        group by
            s_name
        order by
            numwait desc,
            s_name limit 100;
--Q22: global sales opportunity query
explain select
            cntrycode,
            count(*) as numcust,
            sum(c_acctbal) as totacctbal
        from
            (
                select
                    substring(c_phone from 1 for 2) as cntrycode,
                    c_acctbal
                from
                    customer
                where
                        substring(c_phone from 1 for 2) in
                        ('26', '20', '32', '21', '25', '29', '16')
                  and c_acctbal > (
                    select
                        avg(c_acctbal)
                    from
                        customer
                    where
                            c_acctbal > 0.00
                      and substring(c_phone from 1 for 2) in
                          ('26', '20', '32', '21', '25', '29', '16')
                )
                  and not exists (
                    select
                        *
                    from
                        orders
                    where
                            o_custkey = c_custkey
                )
            ) as custsale
        group by
            cntrycode
        order by
            cntrycode limit 1;

INSERT INTO LINEITEM
VALUES
    ('2000-01-01 01:00:00', 1, 156, 4, 1, 17, 17954.55, 0.04, 0.02, 'N', 'O', '1996-03-13', '1996-02-12', '1996-03-22', 'DELIVER IN PERSON', 'TRUCK', 'egular courts above the',  NULL, 1),
    ('2000-01-01 01:00:01', 1, 68, 9, 2, 36, 34850.16, 0.09, 0.06, 'N', 'O', '1996-04-12', '1996-02-28', '1996-04-20', 'TAKE BACK RETURN', 'MAIL', 'ly final dependencies: slyly bold',  NULL, 2),
    ('2000-01-01 01:00:02', 1, 64, 5, 3, 8, 7712.48, 0.10, 0.02, 'N', 'O', '1996-01-29', '1996-03-05', '1996-01-31', 'TAKE BACK RETURN', 'REG AIR', 'riously. regular, express dep',  NULL, 3),
    ('2000-01-01 01:00:03', 1, 3, 6, 4, 28, 25284.00, 0.09, 0.06, 'N', 'O', '1996-04-21', '1996-03-30', '1996-05-16', 'NONE', 'AIR', 'lites. fluffily even de',  NULL, 4),
    ('2000-01-01 01:00:04', 1, 25, 8, 5, 24, 22200.48, 0.10, 0.04, 'N', 'O', '1996-03-30', '1996-03-14', '1996-04-01', 'NONE', 'FOB', 'pending foxes. slyly re',  NULL, 5),
    ('2000-01-01 01:00:05', 1, 16, 3, 6, 32, 29312.32, 0.07, 0.02, 'N', 'O', '1996-01-30', '1996-02-07', '1996-02-03', 'DELIVER IN PERSON', 'MAIL', 'arefully slyly ex',  NULL, 6),
    ('2000-01-01 01:00:06', 2, 107, 2, 1, 38, 38269.80, 0.00, 0.05, 'N', 'O', '1997-01-28', '1997-01-14', '1997-02-02', 'TAKE BACK RETURN', 'RAIL', 'ven requests. deposits breach a',  NULL, 7),
    ('2000-01-01 01:00:07', 3, 5, 2, 1, 45, 40725.00, 0.06, 0.00, 'R', 'F', '1994-02-02', '1994-01-04', '1994-02-23', 'NONE', 'AIR', 'ongside of the furiously brave acco',  NULL, 8),
    ('2000-01-01 01:00:08', 3, 20, 10, 2, 49, 45080.98, 0.10, 0.00, 'R', 'F', '1993-11-09', '1993-12-20', '1993-11-24', 'TAKE BACK RETURN', 'RAIL', 'unusual accounts. eve',  NULL, 9),
    ('2000-01-01 01:00:09', 3, 129, 8, 3, 27, 27786.24, 0.06, 0.07, 'A', 'F', '1994-01-16', '1993-11-22', '1994-01-23', 'DELIVER IN PERSON', 'SHIP', 'nal foxes wake. ',  NULL, 10),
    ('2000-01-01 01:00:10', 3, 30, 5, 4, 2, 1860.06, 0.01, 0.06, 'A', 'F', '1993-12-04', '1994-01-07', '1994-01-01', 'NONE', 'TRUCK', 'y. fluffily pending d',  NULL, 11),
    ('2000-01-01 01:00:11', 3, 184, 5, 5, 28, 30357.04, 0.04, 0.00, 'R', 'F', '1993-12-14', '1994-01-10', '1994-01-01', 'TAKE BACK RETURN', 'FOB', 'ages nag slyly pending',  NULL, 12),
    ('2000-01-01 01:00:12', 3, 63, 8, 6, 26, 25039.56, 0.10, 0.02, 'A', 'F', '1993-10-29', '1993-12-18', '1993-11-04', 'TAKE BACK RETURN', 'RAIL', 'ges sleep after the caref',  NULL, 13),
    ('2000-01-01 01:00:13', 4, 89, 10, 1, 30, 29672.40, 0.03, 0.08, 'N', 'O', '1996-01-10', '1995-12-14', '1996-01-18', 'DELIVER IN PERSON', 'REG AIR', '- quickly regular packages sleep. idly',  NULL, 14),
    ('2000-01-01 01:00:14', 5, 109, 10, 1, 15, 15136.50, 0.02, 0.04, 'R', 'F', '1994-10-31', '1994-08-31', '1994-11-20', 'NONE', 'AIR', 'ts wake furiously ',  NULL, 15),
    ('2000-01-01 01:00:15', 5, 124, 5, 2, 26, 26627.12, 0.07, 0.08, 'R', 'F', '1994-10-16', '1994-09-25', '1994-10-19', 'NONE', 'FOB', 'sts use slyly quickly special instruc',  NULL, 16),
    ('2000-01-01 01:00:16', 5, 38, 4, 3, 50, 46901.50, 0.08, 0.03, 'A', 'F', '1994-08-08', '1994-10-13', '1994-08-26', 'DELIVER IN PERSON', 'AIR', 'eodolites. fluffily unusual',  NULL, 17),
    ('2000-01-01 01:00:17', 6, 140, 6, 1, 37, 38485.18, 0.08, 0.03, 'A', 'F', '1992-04-27', '1992-05-15', '1992-05-02', 'TAKE BACK RETURN', 'TRUCK', 'p furiously special foxes',  NULL, 18),
    ('2000-01-01 01:00:18', 7, 183, 4, 1, 12, 12998.16, 0.07, 0.03, 'N', 'O', '1996-05-07', '1996-03-13', '1996-06-03', 'TAKE BACK RETURN', 'FOB', 'ss pinto beans wake against th',  NULL, 19),
    ('2000-01-01 01:00:19', 7, 146, 3, 2, 9, 9415.26, 0.08, 0.08, 'N', 'O', '1996-02-01', '1996-03-02', '1996-02-19', 'TAKE BACK RETURN', 'SHIP', 'es. instructions', NULL, 20),
    ('2000-01-01 01:00:20', 7, 95, 8, 3, 46, 45774.14, 0.10, 0.07, 'N', 'O', '1996-01-15', '1996-03-27', '1996-02-03', 'COLLECT COD', 'MAIL', ' unusual reques',  NULL, 21),
    ('2000-01-01 01:00:21', 7, 164, 5, 4, 28, 29796.48, 0.03, 0.04, 'N', 'O', '1996-03-21', '1996-04-08', '1996-04-20', 'NONE', 'FOB', '. slyly special requests haggl',  NULL, 22),
    ('2000-01-01 01:00:22', 7, 152, 4, 5, 38, 39981.70, 0.08, 0.01, 'N', 'O', '1996-02-11', '1996-02-24', '1996-02-18', 'DELIVER IN PERSON', 'TRUCK', 'ns haggle carefully ironic deposits. bl',  NULL, 23),
    ('2000-01-01 01:00:23', 7, 80, 10, 6, 35, 34302.80, 0.06, 0.03, 'N', 'O', '1996-01-16', '1996-02-23', '1996-01-22', 'TAKE BACK RETURN', 'FOB', 'jole. excuses wake carefully alongside of ',  NULL, 24),
    ('2000-01-01 01:00:24', 7, 158, 3, 7, 5, 5290.75, 0.04, 0.02, 'N', 'O', '1996-02-10', '1996-03-26', '1996-02-13', 'NONE', 'FOB', 'ithely regula', NULL, 25),
    ('2000-01-01 01:00:25', 32, 83, 4, 1, 28, 27526.24, 0.05, 0.08, 'N', 'O', '1995-10-23', '1995-08-27', '1995-10-26', 'TAKE BACK RETURN', 'TRUCK', 'sleep quickly. req',  NULL, 26),
    ('2000-01-01 01:00:26', 32, 198, 10, 2, 32, 35142.08, 0.02, 0.00, 'N', 'O', '1995-08-14', '1995-10-07', '1995-08-27', 'COLLECT COD', 'AIR', 'lithely regular deposits. fluffily ',  NULL, 27),
    ('2000-01-01 01:00:27', 32, 45, 2, 3, 2, 1890.08, 0.09, 0.02, 'N', 'O', '1995-08-07', '1995-10-07', '1995-08-23', 'DELIVER IN PERSON', 'AIR', ' express accounts wake according to the',  NULL, 28),
    ('2000-01-01 01:00:28', 32, 3, 8, 4, 4, 3612.00, 0.09, 0.03, 'N', 'O', '1995-08-04', '1995-10-01', '1995-09-03', 'NONE', 'REG AIR', 'e slyly final pac',  NULL, 29),
    ('2000-01-01 01:00:29', 32, 86, 7, 5, 44, 43387.52, 0.05, 0.06, 'N', 'O', '1995-08-28', '1995-08-20', '1995-09-14', 'DELIVER IN PERSON', 'AIR', 'symptotes nag according to the ironic depo',  NULL, 30),
    ('2000-01-01 01:00:30', 32, 12, 6, 6, 6, 5472.06, 0.04, 0.03, 'N', 'O', '1995-07-21', '1995-09-23', '1995-07-25', 'COLLECT COD', 'RAIL', ' gifts cajole carefully.',  NULL, 31),
    ('2000-01-01 01:00:31', 33, 62, 7, 1, 31, 29823.86, 0.09, 0.04, 'A', 'F', '1993-10-29', '1993-12-19', '1993-11-08', 'COLLECT COD', 'TRUCK', 'ng to the furiously ironic package',  NULL, 32),
    ('2000-01-01 01:00:32', 33, 61, 8, 2, 32, 30753.92, 0.02, 0.05, 'A', 'F', '1993-12-09', '1994-01-04', '1993-12-28', 'COLLECT COD', 'MAIL', 'gular theodolites',  NULL, 33),
    ('2000-01-01 01:00:33', 33, 138, 4, 3, 5, 5190.65, 0.05, 0.03, 'A', 'F', '1993-12-09', '1993-12-25', '1993-12-23', 'TAKE BACK RETURN', 'AIR', '. stealthily bold exc',  NULL, 34),
    ('2000-01-01 01:00:34', 33, 34, 5, 4, 41, 38295.23, 0.09, 0.00, 'R', 'F', '1993-11-09', '1994-01-24', '1993-11-11', 'TAKE BACK RETURN', 'MAIL', 'unusual packages doubt caref',  NULL, 35),
    ('2000-01-01 01:00:35', 34, 89, 10, 1, 13, 12858.04, 0.00, 0.07, 'N', 'O', '1998-10-23', '1998-09-14', '1998-11-06', 'NONE', 'REG AIR', 'nic accounts. deposits are alon', NULL, 36),
    ('2000-01-01 01:00:36', 34, 90, 1, 2, 22, 21781.98, 0.08, 0.06, 'N', 'O', '1998-10-09', '1998-10-16', '1998-10-12', 'NONE', 'FOB', 'thely slyly p',  NULL, 37),
    ('2000-01-01 01:00:37', 34, 170, 7, 3, 6, 6421.02, 0.02, 0.06, 'N', 'O', '1998-10-30', '1998-09-20', '1998-11-05', 'NONE', 'FOB', 'ar foxes sleep ',  NULL, 38),
    ('2000-01-01 01:00:38', 35, 1, 4, 1, 24, 21624.00, 0.02, 0.00, 'N', 'O', '1996-02-21', '1996-01-03', '1996-03-18', 'TAKE BACK RETURN', 'FOB', ', regular tithe',  NULL, 39),
    ('2000-01-01 01:00:39', 35, 162, 1, 2, 34, 36113.44, 0.06, 0.08, 'N', 'O', '1996-01-22', '1996-01-06', '1996-01-27', 'DELIVER IN PERSON', 'RAIL', 's are carefully against the f',  NULL, 40),
    ('2000-01-01 01:00:40', 35, 121, 4, 3, 7, 7147.84, 0.06, 0.04, 'N', 'O', '1996-01-19', '1995-12-22', '1996-01-29', 'NONE', 'MAIL', ' the carefully regular ',  NULL, 41),
    ('2000-01-01 01:00:41', 35, 86, 7, 4, 25, 24652.00, 0.06, 0.05, 'N', 'O', '1995-11-26', '1995-12-25', '1995-12-21', 'DELIVER IN PERSON', 'SHIP', ' quickly unti',  NULL, 42),
    ('2000-01-01 01:00:42', 35, 120, 7, 5, 34, 34684.08, 0.08, 0.06, 'N', 'O', '1995-11-08', '1996-01-15', '1995-11-26', 'COLLECT COD', 'MAIL', '. silent, unusual deposits boost', NULL, 43),
    ('2000-01-01 01:00:43', 35, 31, 7, 6, 28, 26068.84, 0.03, 0.02, 'N', 'O', '1996-02-01', '1995-12-24', '1996-02-28', 'COLLECT COD', 'RAIL', 'ly alongside of ', NULL, 44),
    ('2000-01-01 01:00:44', 36, 120, 1, 1, 42, 42845.04, 0.09, 0.00, 'N', 'O', '1996-02-03', '1996-01-21', '1996-02-23', 'COLLECT COD', 'SHIP', ' careful courts. special ', NULL, 45),
    ('2000-01-01 01:00:45', 37, 23, 8, 1, 40, 36920.80, 0.09, 0.03, 'A', 'F', '1992-07-21', '1992-08-01', '1992-08-15', 'NONE', 'REG AIR', 'luffily regular requests. slyly final acco', NULL, 46),
    ('2000-01-01 01:00:46', 37, 127, 6, 2, 39, 40057.68, 0.05, 0.02, 'A', 'F', '1992-07-02', '1992-08-18', '1992-07-28', 'TAKE BACK RETURN', 'RAIL', 'the final requests. ca', NULL, 47),
    ('2000-01-01 01:00:47', 37, 13, 7, 3, 43, 39259.43, 0.05, 0.08, 'A', 'F', '1992-07-10', '1992-07-06', '1992-08-02', 'DELIVER IN PERSON', 'TRUCK', 'iously ste', NULL, 48),
    ('2000-01-01 01:00:48', 38, 176, 5, 1, 44, 47351.48, 0.04, 0.02, 'N', 'O', '1996-09-29', '1996-11-17', '1996-09-30', 'COLLECT COD', 'MAIL', 's. blithely unusual theodolites am', NULL, 49),
    ('2000-01-01 01:00:49', 39, 3, 10, 1, 44, 39732.00, 0.09, 0.06, 'N', 'O', '1996-11-14', '1996-12-15', '1996-12-12', 'COLLECT COD', 'RAIL', 'eodolites. careful',NULL, 50),
    ('2000-01-01 01:00:50', 39, 187, 8, 2, 26, 28266.68, 0.08, 0.04, 'N', 'O', '1996-11-04', '1996-10-20', '1996-11-20', 'NONE', 'FOB', 'ckages across the slyly silent', NULL, 51),
    ('2000-01-01 01:00:51', 39, 68, 3, 3, 46, 44530.76, 0.06, 0.08, 'N', 'O', '1996-09-26', '1996-12-19', '1996-10-26', 'DELIVER IN PERSON', 'AIR', 'he carefully e', NULL, 52),
    ('2000-01-01 01:00:52', 39, 21, 6, 4, 32, 29472.64, 0.07, 0.05, 'N', 'O', '1996-10-02', '1996-12-19', '1996-10-14', 'COLLECT COD', 'MAIL', 'heodolites sleep silently pending foxes. ac', NULL, 53),
    ('2000-01-01 01:00:53', 39, 55, 10, 5, 43, 41067.15, 0.01, 0.01, 'N', 'O', '1996-10-17', '1996-11-14', '1996-10-26', 'COLLECT COD', 'MAIL', 'yly regular i', NULL, 54),
    ('2000-01-01 01:00:54', 39, 95, 7, 6, 40, 39803.60, 0.06, 0.05, 'N', 'O', '1996-12-08', '1996-10-22', '1997-01-01', 'COLLECT COD', 'AIR', 'quickly ironic fox', NULL, 55),
    ('2000-01-01 01:00:55', 64, 86, 7, 1, 21, 20707.68, 0.05, 0.02, 'R', 'F', '1994-09-30', '1994-09-18', '1994-10-26', 'DELIVER IN PERSON', 'REG AIR', 'ch slyly final, thin platelets.', NULL, 56),
    ('2000-01-01 01:00:56', 65, 60, 5, 1, 26, 24961.56, 0.03, 0.03, 'A', 'F', '1995-04-20', '1995-04-25', '1995-05-13', 'NONE', 'TRUCK', 'pending deposits nag even packages. ca', NULL, 57),
    ('2000-01-01 01:00:57', 65, 74, 3, 2, 22, 21429.54, 0.00, 0.05, 'N', 'O', '1995-07-17', '1995-06-04', '1995-07-19', 'COLLECT COD', 'FOB', ' ideas. special, r', NULL, 58),
    ('2000-01-01 01:00:58', 65, 2, 5, 3, 21, 18942.00, 0.09, 0.07, 'N', 'O', '1995-07-06', '1995-05-14', '1995-07-31', 'DELIVER IN PERSON', 'RAIL', 'bove the even packages. accounts nag carefu', NULL, 59),
    ('2000-01-01 01:00:59', 66, 116, 10, 1, 31, 31499.41, 0.00, 0.08, 'R', 'F', '1994-02-19', '1994-03-11', '1994-02-20', 'TAKE BACK RETURN', 'RAIL', 'ut the unusual accounts sleep at the bo', NULL, 60);

INSERT INTO ORDERS
VALUES
    ('2000-01-01 01:00:00', 1, 37, 'O', 131251.81, '1996-01-02', '5-LOW', 'Clerk#000000951', 0, 'nstructions sleep furiously among ', NULL, 1),
    ('2000-01-01 01:00:01', 2, 79, 'O', 40183.29, '1996-12-01', '1-URGENT', 'Clerk#000000880', 0, ' foxes. pending accounts at the pending, silent asymptot', NULL, 2),
    ('2000-01-01 01:00:02', 3, 124, 'F', 160882.76, '1993-10-14', '5-LOW', 'Clerk#000000955', 0, 'sly final accounts boost. carefully regular ideas cajole carefully. depos', NULL, 3),
    ('2000-01-01 01:00:03', 4, 137, 'O', 31084.79, '1995-10-11', '5-LOW', 'Clerk#000000124', 0, 'sits. slyly regular warthogs cajole. regular, regular theodolites acro', NULL, 4),
    ('2000-01-01 01:00:04', 5, 46, 'F', 86615.25, '1994-07-30', '5-LOW', 'Clerk#000000925', 0, 'quickly. bold deposits sleep slyly. packages use slyly', NULL, 5),
    ('2000-01-01 01:00:05', 6, 56, 'F', 36468.55, '1992-02-21', '4-NOT SPECIFIED', 'Clerk#000000058', 0, 'ggle. special, final requests are against the furiously specia', NULL, 6),
    ('2000-01-01 01:00:06', 7, 40, 'O', 171488.73, '1996-01-10', '2-HIGH', 'Clerk#000000470', 0, 'ly special requests ', NULL, 7),
    ('2000-01-01 01:00:07', 32, 131, 'O', 116923.00, '1995-07-16', '2-HIGH', 'Clerk#000000616', 0, 'ise blithely bold, regular requests. quickly unusual dep', NULL, 8),
    ('2000-01-01 01:00:08', 33, 67, 'F', 99798.76, '1993-10-27', '3-MEDIUM', 'Clerk#000000409', 0, 'uriously. furiously final request', NULL, 9),
    ('2000-01-01 01:00:09', 34, 62, 'O', 41670.02, '1998-07-21', '3-MEDIUM', 'Clerk#000000223', 0, 'ly final packages. fluffily final deposits wake blithely ideas. spe', NULL, 10),
    ('2000-01-01 01:00:10', 35, 128, 'O', 148789.52, '1995-10-23', '4-NOT SPECIFIED', 'Clerk#000000259', 0, 'zzle. carefully enticing deposits nag furio', NULL, 11),
    ('2000-01-01 01:00:11', 36, 116, 'O', 38988.98, '1995-11-03', '1-URGENT', 'Clerk#000000358', 0, ' quick packages are blithely. slyly silent accounts wake qu', NULL, 12),
    ('2000-01-01 01:00:12', 37, 88, 'F', 113701.89, '1992-06-03', '3-MEDIUM', 'Clerk#000000456', 0, 'kly regular pinto beans. carefully unusual waters cajole never', NULL, 13),
    ('2000-01-01 01:00:13', 38, 125, 'O', 46366.56, '1996-08-21', '4-NOT SPECIFIED', 'Clerk#000000604', 0, 'haggle blithely. furiously express ideas haggle blithely furiously regular re', NULL, 14),
    ('2000-01-01 01:00:14', 39, 82, 'O', 219707.84, '1996-09-20', '3-MEDIUM', 'Clerk#000000659', 0, 'ole express, ironic requests: ir', NULL, 15);

INSERT INTO CUSTOMER
VALUES
    ('2000-01-01 01:00:00', 1, 'Customer#000000001', 'IVhzIApeRb ot,c,E', 15, '25-989-741-2988', 711.56, 'BUILDING', 'to the even, regular platelets. regular, ironic epitaphs nag e', NULL,1),
    ('2000-01-01 01:00:01', 2, 'Customer#000000002', 'XSTf4,NCwDVaWNe6tEgvwfmRchLXak', 13, '23-768-687-3665', 121.65, 'AUTOMOBILE', 'l accounts. blithely ironic theodolites integrate boldly: caref', NULL,2),
    ('2000-01-01 01:00:02', 3, 'Customer#000000003', 'MG9kdTD2WBHm', 1, '11-719-748-3364', 7498.12, 'AUTOMOBILE', ' deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov', NULL,3),
    ('2000-01-01 01:00:03', 4, 'Customer#000000004', 'XxVSJsLAGtn', 4, '14-128-190-5944', 2866.83, 'MACHINERY', ' requests. final, regular ideas sleep final accou', NULL,4),
    ('2000-01-01 01:00:04', 5, 'Customer#000000005', 'KvpyuHCplrB84WgAiGV6sYpZq7Tj', 3, '13-750-942-6364', 794.47, 'HOUSEHOLD', 'n accounts will have to unwind. foxes cajole accor', NULL,5);

INSERT INTO PARTSUPP
VALUES
    ('2000-01-01 01:00:00', 1, 2, 3325, 771.64, ', even theodolites. regular, final theodolites eat after the carefully pending foxes. furiously regular deposits sleep slyly. carefully bold realms above the ironic dependencies haggle careful', NULL,1),
    ('2000-01-01 01:00:01', 1, 4, 8076, 993.49, 'ven ideas. quickly even packages print. pending multipliers must have to are fluff', NULL,2),
    ('2000-01-01 01:00:02', 1, 6, 3956, 337.09, 'after the fluffily ironic deposits? blithely special dependencies integrate furiously even excuses. blithely silent theodolites could have to haggle pending, express requests; fu', NULL,3),
    ('2000-01-01 01:00:03', 1, 8, 4069, 357.84, 'al, regular dependencies serve carefully after the quickly final pinto beans. furiously even deposits sleep quickly final, silent pinto beans. fluffily reg', NULL,4),
    ('2000-01-01 01:00:04', 2, 3, 8895, 378.49, 'nic accounts. final accounts sleep furiously about the ironic, bold packages. regular, regular accounts', NULL,5),
    ('2000-01-01 01:00:05', 2, 5, 4969, 915.27, 'ptotes. quickly pending dependencies integrate furiously. fluffily ironic ideas impress blithely above the express accounts. furiously even epitaphs need to wak', NULL,6),
    ('2000-01-01 01:00:06', 2, 7, 8539, 438.37, 'blithely bold ideas. furiously stealthy packages sleep fluffily. slyly special deposits snooze furiously carefully regular accounts. regular deposits according to the accounts nag carefully slyl', NULL,7),
    ('2000-01-01 01:00:07', 2, 9, 3025, 306.39, 'olites. deposits wake carefully. even, express requests cajole. carefully regular ex', NULL,8);

INSERT INTO PART
VALUES
    ('2000-01-01 01:00:00', 1, 'goldenrod lavender spring chocolate lace', 'Manufacturer#1', 'Brand#13', 'PROMO BURNISHED COPPER', 7, 'JUMBO PKG', 901.00, 'ly. slyly ironi', NULL,1),
    ('2000-01-01 01:00:01', 2, 'blush thistle blue yellow saddle', 'Manufacturer#1', 'Brand#13', 'LARGE BRUSHED BRASS', 1, 'LG CASE', 902.00, 'lar accounts amo', NULL,2),
    ('2000-01-01 01:00:02', 3, 'spring green yellow purple cornsilk', 'Manufacturer#4', 'Brand#42', 'STANDARD POLISHED BRASS', 21, 'WRAP CASE', 903.00, 'egular deposits hag', NULL,3),
    ('2000-01-01 01:00:03', 4, 'cornflower chocolate smoke green pink', 'Manufacturer#3', 'Brand#34', 'SMALL PLATED BRASS', 14, 'MED DRUM', 904.00, 'p furiously r', NULL,4),
    ('2000-01-01 01:00:04', 5, 'forest brown coral puff cream', 'Manufacturer#3', 'Brand#32', 'STANDARD POLISHED TIN', 15, 'SM PKG', 905.00, ' wake carefully ', NULL,5),
    ('2000-01-01 01:00:05', 6, 'bisque cornflower lawn forest magenta', 'Manufacturer#2', 'Brand#24', 'PROMO PLATED STEEL', 4, 'MED BAG', 906.00, 'sual a', NULL,6),
    ('2000-01-01 01:00:06', 7, 'moccasin green thistle khaki floral', 'Manufacturer#1', 'Brand#11', 'SMALL PLATED COPPER', 45, 'SM BAG', 907.00, 'lyly. ex', NULL,7);

INSERT INTO SUPPLIER
VALUES
    ('2000-01-01 01:00:00', 1, 'Supplier#000000001', ' N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ', 17, '27-918-335-1736', 5755.94, 'each slyly above the careful', NULL,1),
    ('2000-01-01 01:00:01', 2, 'Supplier#000000002', '89eJ5ksX3ImxJQBvxObC,', 5, '15-679-861-2259', 4032.68, ' slyly bold instructions. idle dependen', NULL,2),
    ('2000-01-01 01:00:02', 3, 'Supplier#000000003', 'q1,G3Pj6OjIuUYfUoH18BFTKP5aU9bEV3', 1, '11-383-516-1199', 4192.40, 'blithely silent requests after the express dependencies are sl', NULL,3);


INSERT INTO REGION
VALUES
    ('2000-01-01 01:00:00', 0, 'AFRICA', 'lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to ', NULL,1),
    ('2000-01-01 01:00:01', 1, 'AMERICA', 'hs use ironic, even requests. s', NULL,2),
    ('2000-01-01 01:00:02', 2, 'ASIA', 'ges. thinly even pinto beans ca', NULL,3),
    ('2000-01-01 01:00:03', 3, 'EUROPE', 'ly final courts cajole furiously final excuse', NULL,4),
    ('2000-01-01 01:00:04', 4, 'MIDDLE EAST', 'uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl', NULL,5);

INSERT INTO NATION
VALUES
    ('2000-01-01 01:00:00', 0, 'ALGERIA', 0, ' haggle. carefully final deposits detect slyly agai', NULL,1),
    ('2000-01-01 01:00:01', 1, 'ARGENTINA', 1, 'al foxes promise slyly according to the regular accounts. bold requests alon', NULL,2),
    ('2000-01-01 01:00:02', 2, 'BRAZIL', 1, 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ', NULL,3),
    ('2000-01-01 01:00:03', 3, 'CANADA', 1, 'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold', NULL,4),
    ('2000-01-01 01:00:04', 4, 'EGYPT', 4, 'y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d', NULL,5),
    ('2000-01-01 01:00:05', 5, 'ETHIOPIA', 0, 'ven packages wake quickly. regu', NULL,6),
    ('2000-01-01 01:00:06', 6, 'FRANCE', 3, 'refully final requests. regular, ironi', NULL,7),
    ('2000-01-01 01:00:07', 7, 'GERMANY', 3, 'l platelets. regular accounts x-ray: unusual, regular acco', NULL,8);

-- stats
create statistics st_nation from NATION;
create statistics st_region from region;
create statistics st_supplier from supplier;
create statistics st_part from part;
create statistics st_partsupp from partsupp;
create statistics st_customer from customer;
create statistics st_orders from orders;
create statistics st_lineitem from lineitem;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st_nation';
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st_region';
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st_supplier';
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st_partsupp';
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st_customer';
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st_orders';
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st_lineitem';

-- Q1 - price statistics report
explain explain select
                                                                            l_returnflag,
                                                                            l_linestatus,
                                                                            sum(l_quantity) as sum_qty,
                                                                            sum(l_extendedprice) as sum_base_price,
                                                                            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                                                                            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                                                                            avg(l_quantity) as avg_qty,
                                                                            avg(l_extendedprice) as avg_price,
                                                                            avg(l_discount) as avg_disc,
                                                                            count(*) as count_order
                from
                                                                            lineitem
                where
                                                                                l_shipdate <= date '1998-12-01' - interval '79' day
                group by
                    l_returnflag,
                    l_linestatus
                order by
                    l_returnflag,
                    l_linestatus limit 1;
--Q2: minimum cost supplier query
explain select
            s_acctbal,
            s_name,
            n_name,
            p_partkey,
            p_mfgr,
            s_address,
            s_phone,
            s_comment
        from
            part,
            supplier,
            partsupp,
            nation,
            region
        where
                p_partkey = ps_partkey
          and s_suppkey = ps_suppkey
          and p_size = 12
          and p_type like '%NICKEL'
          and s_nationkey = n_nationkey
          and n_regionkey = r_regionkey
          and r_name = 'MIDDLE EAST'
          and ps_supplycost = (
            select
                min(ps_supplycost)
            from
                partsupp,
                supplier,
                nation,
                region
            where
                    p_partkey = ps_partkey
              and s_suppkey = ps_suppkey
              and s_nationkey = n_nationkey
              and n_regionkey = r_regionkey
              and r_name = 'MIDDLE EAST'
        )
        order by
            s_acctbal desc,
            n_name,
            s_name,
            p_partkey limit 100;
--Q3: shipping priority query
explain select
            l_orderkey,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            o_orderdate,
            o_shippriority
        from
            customer,
            orders,
            lineitem
        where
                c_mktsegment = 'AUTOMOBILE'
          and c_custkey = o_custkey
          and l_orderkey = o_orderkey
          and o_orderdate < date '1995-03-27'
          and l_shipdate > date '1995-03-27'
        group by
            l_orderkey,
            o_orderdate,
            o_shippriority
        order by
            revenue desc,
            o_orderdate limit 10;
--Q4: order priority query
explain select
            o_orderpriority,
            count(*) as order_count
        from
            orders
        where
                o_orderdate >= date '1997-01-01'
          and o_orderdate < date '1997-01-01' + interval '3' month
          and exists (
            select
                *
            from
                lineitem
            where
                    l_orderkey = o_orderkey
              and l_commitdate < l_receiptdate
        )
        group by
            o_orderpriority
        order by
            o_orderpriority limit 1;
--Q5: query the revenue that a supplier brings to the company in a certain region
explain select
            n_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue
        from
            customer,
            orders,
            lineitem,
            supplier,
            nation,
            region
        where
                c_custkey = o_custkey
          and l_orderkey = o_orderkey
          and l_suppkey = s_suppkey
          and c_nationkey = s_nationkey
          and s_nationkey = n_nationkey
          and n_regionkey = r_regionkey
          and r_name = 'MIDDLE EAST'
          and o_orderdate >= date '1994-01-01'
          and o_orderdate < date '1994-01-01' + interval '1' year
        group by
            n_name
        order by
            revenue desc limit 1;
--Q6: forecast revenue change query
explain select
            sum(l_extendedprice * l_discount) as revenue
        from
            lineitem
        where
                l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
          and l_discount between 0.04 - 0.01 and 0.04 + 0.01
          and l_quantity < 24 limit 1;
--Q7: freight Profitability query
explain select
            supp_nation,
            cust_nation,
            l_year,
            sum(volume) as revenue
        from
            (
                select
                    n1.n_name as supp_nation,
                    n2.n_name as cust_nation,
                    extract(year from l_shipdate) as l_year,
                    l_extendedprice * (1 - l_discount) as volume
                from
                    supplier,
                    lineitem,
                    orders,
                    customer,
                    nation n1,
                    nation n2
                where
                        s_suppkey = l_suppkey
                  and o_orderkey = l_orderkey
                  and c_custkey = o_custkey
                  and s_nationkey = n1.n_nationkey
                  and c_nationkey = n2.n_nationkey
                  and (
                        (n1.n_name = 'FRANCE' and n2.n_name = 'CANADA')
                        or (n1.n_name = 'CANADA' and n2.n_name = 'FRANCE')
                    )
                  and l_shipdate between date '1995-01-01' and date '1996-12-31'
            ) as shipping
        group by
            supp_nation,
            cust_nation,
            l_year
        order by
            supp_nation,
            cust_nation,
            l_year limit 1;
--Q8: country market share Query
explain select
            o_year,
            sum(case
                    when nation = 'CANADA' then volume
                    else 0
                end) / sum(volume) as mkt_share
        from
            (
                select
                    extract(year from o_orderdate) as o_year,
                    l_extendedprice * (1 - l_discount) as volume,
                    n2.n_name as nation
                from
                    part,
                    supplier,
                    lineitem,
                    orders,
                    customer,
                    nation n1,
                    nation n2,
                    region
                where
                        p_partkey = l_partkey
                  and s_suppkey = l_suppkey
                  and l_orderkey = o_orderkey
                  and o_custkey = c_custkey
                  and c_nationkey = n1.n_nationkey
                  and n1.n_regionkey = r_regionkey
                  and r_name = 'AMERICA'
                  and s_nationkey = n2.n_nationkey
                  and o_orderdate between date '1995-01-01' and date '1996-12-31'
                  and p_type = 'SMALL POLISHED STEEL'
            ) as all_nations
        group by
            o_year
        order by
            o_year limit 1;
--Q9: product type profit estimation Query
explain select
            nation,
            o_year,
            sum(amount) as sum_profit
        from
            (
                select
                    n_name as nation,
                    extract(year from o_orderdate) as o_year,
                    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                from
                    part,
                    supplier,
                    lineitem,
                    partsupp,
                    orders,
                    nation
                where
                        s_suppkey = l_suppkey
                  and ps_suppkey = l_suppkey
                  and ps_partkey = l_partkey
                  and p_partkey = l_partkey
                  and o_orderkey = l_orderkey
                  and s_nationkey = n_nationkey
                  and p_name like '%firebrick%'
            ) as profit
        group by
            nation,
            o_year
        order by
            nation,
            o_year desc limit 1;
--Q10: query of freight problems
explain select
            c_custkey,
            c_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            c_acctbal,
            n_name,
            c_address,
            c_phone,
            c_comment
        from
            customer,
            orders,
            lineitem,
            nation
        where
                c_custkey = o_custkey
          and l_orderkey = o_orderkey
          and o_orderdate >= date '1993-04-01'
          and o_orderdate < date '1993-04-01' + interval '3' month
          and l_returnflag = 'R'
          and c_nationkey = n_nationkey
        group by
            c_custkey,
            c_name,
            c_acctbal,
            c_phone,
            n_name,
            c_address,
            c_comment
        order by
            revenue desc limit 20;
--Q11: inventory value query
explain select
            ps_partkey,
            sum(ps_supplycost * ps_availqty) as value
        from
            partsupp,
            supplier,
            nation
        where
            ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'ETHIOPIA'
        group by
            ps_partkey having
            sum(ps_supplycost * ps_availqty) > (
            select
            sum(ps_supplycost * ps_availqty) * 0.0001000000
            from
            partsupp,
            supplier,
            nation
            where
            ps_suppkey = s_suppkey
                          and s_nationkey = n_nationkey
                          and n_name = 'FRANCE'
            )
        order by
            value desc limit 1;
--Q12: shipping mode and order priority query
explain select
            l_shipmode,
            sum(case
                    when o_orderpriority = '1-URGENT'
                        or o_orderpriority = '2-HIGH'
                        then 1
                    else 0
                end) as high_line_count,
            sum(case
                    when o_orderpriority <> '1-URGENT'
                        and o_orderpriority <> '2-HIGH'
                        then 1
                    else 0
                end) as low_line_count
        from
            orders,
            lineitem
        where
                o_orderkey = l_orderkey
          and l_shipmode in ('FOB', 'TRUCK')
          and l_commitdate < l_receiptdate
          and l_shipdate < l_commitdate
          and l_receiptdate >= date '1997-01-01'
          and l_receiptdate < date '1997-01-01' + interval '1' year
        group by
            l_shipmode
        order by
            l_shipmode limit 1;
--Q13: consumer order quantity query
explain select
            c_count,
            count(*) as custdist
        from
            (
                select
                    c_custkey,
                    count(o_orderkey)
                from
                    customer left outer join orders on
                                c_custkey = o_custkey
                            and o_comment not like '%pending%deposits%'
                group by
                    c_custkey
            ) as c_orders (c_custkey, c_count)
        group by
            c_count
        order by
            custdist desc,
            c_count desc limit 1;
--Q14: promotion effect query
explain select
                    100.00 * sum(case
                                     when p_type like 'PROMO%'
                                         then l_extendedprice * (1 - l_discount)
                                     else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
        from
            lineitem,
            part
        where
                l_partkey = p_partkey
          and l_shipdate >= date '1997-01-01'
          and l_shipdate < date '1997-01-01' + interval '1' month limit 1;
--Q16: parts/supplier relationship query
explain select
            p_brand,
            p_type,
            p_size,
            count(distinct ps_suppkey) as supplier_cnt
        from
            partsupp,
            part
        where
                p_partkey = ps_partkey
          and p_brand <> 'Brand#53'
          and p_type not like 'SMALL PLATED%'
          and p_size in (13, 46, 9, 35, 22, 2, 24, 31)
          and ps_suppkey not in (
            select
                s_suppkey
            from
                supplier
            where
                    s_comment like '%Customer%Complaints%'
        )
        group by
            p_brand,
            p_type,
            p_size
        order by
            supplier_cnt desc,
            p_brand,
            p_type,
            p_size limit 1;
--Q17: small order revenue query
explain select
                sum(l_extendedprice) / 7.0 as avg_yearly
        from
            lineitem,
            part
        where
                p_partkey = l_partkey
          and p_brand = 'Brand#32'
          and p_container = 'LG BAG'
          and l_quantity < (
            select
                    0.2 * avg(l_quantity)
            from
                lineitem
            where
                    l_partkey = p_partkey
        ) limit 1;
--Q18: large order customer query
explain select
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice,
            sum(l_quantity)
        from
            customer,
            orders,
            lineitem
        where
                o_orderkey in (
                select
                    l_orderkey
                from
                    lineitem
                group by
                    l_orderkey having
                        sum(l_quantity) > 313
            )
          and c_custkey = o_custkey
          and o_orderkey = l_orderkey
        group by
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice
        order by
            o_totalprice desc,
            o_orderdate limit 100;
--Q19: discount income query
explain select
            sum(l_extendedprice* (1 - l_discount)) as revenue
        from
            lineitem,
            part
        where
            (
                        p_partkey = l_partkey
                    and p_brand = 'Brand#25'
                    and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    and l_quantity >= 5 and l_quantity <= 5 + 10
                    and p_size between 1 and 5
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                )
           or
            (
                        p_partkey = l_partkey
                    and p_brand = 'Brand#14'
                    and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    and l_quantity >= 16 and l_quantity <= 16 + 10
                    and p_size between 1 and 10
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                )
           or
            (
                        p_partkey = l_partkey
                    and p_brand = 'Brand#25'
                    and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    and l_quantity >= 28 and l_quantity <= 28 + 10
                    and p_size between 1 and 15
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                ) limit 1;
--Q20: supplier competitiveness query
explain select
            s_name,
            s_address
        from
            supplier,
            nation
        where
                s_suppkey in (
                select
                    ps_suppkey
                from
                    partsupp
                where
                        ps_partkey in (
                        select
                            p_partkey
                        from
                            part
                        where
                                p_name like 'steel%'
                    )
                  and ps_availqty > (
                    select
                            0.5 * sum(l_quantity)
                    from
                        lineitem
                    where
                            l_partkey = ps_partkey
                      and l_suppkey = ps_suppkey
                      and l_shipdate >= date '1993-01-01'
                      and l_shipdate < date '1993-01-01' + interval '1' year
                )
            )
          and s_nationkey = n_nationkey
          and n_name = 'MOZAMBIQUE'
        order by
            s_name limit 1;
--Q21: unable to deliver on time supplier query
explain select
            s_name,
            count(*) as numwait
        from
            supplier,
            lineitem l1,
            orders,
            nation
        where
                s_suppkey = l1.l_suppkey
          and o_orderkey = l1.l_orderkey
          and o_orderstatus = 'F'
          and l1.l_receiptdate > l1.l_commitdate
          and exists (
            select
                *
            from
                lineitem l2
            where
                    l2.l_orderkey = l1.l_orderkey
              and l2.l_suppkey <> l1.l_suppkey
        )
          and not exists (
            select
                *
            from
                lineitem l3
            where
                    l3.l_orderkey = l1.l_orderkey
              and l3.l_suppkey <> l1.l_suppkey
              and l3.l_receiptdate > l3.l_commitdate
        )
          and s_nationkey = n_nationkey
          and n_name = 'CANADA'
        group by
            s_name
        order by
            numwait desc,
            s_name limit 100;
--Q22: global sales opportunity query
explain select
            cntrycode,
            count(*) as numcust,
            sum(c_acctbal) as totacctbal
        from
            (
                select
                    substring(c_phone from 1 for 2) as cntrycode,
                    c_acctbal
                from
                    customer
                where
                        substring(c_phone from 1 for 2) in
                        ('26', '20', '32', '21', '25', '29', '16')
                  and c_acctbal > (
                    select
                        avg(c_acctbal)
                    from
                        customer
                    where
                            c_acctbal > 0.00
                      and substring(c_phone from 1 for 2) in
                          ('26', '20', '32', '21', '25', '29', '16')
                )
                  and not exists (
                    select
                        *
                    from
                        orders
                    where
                            o_custkey = c_custkey
                )
            ) as custsale
        group by
            cntrycode
        order by
            cntrycode limit 1;
use default;
drop database tpch cascade;
delete  from system.table_statistics where name = 'st_nation' or name = 'st_region' or name = 'st_supplier' or name = 'st_partsupp' or name = 'st_customer' or name = 'st_orders' or name = 'st_lineitem';
set enable_multimodel=on;
set cluster setting ts.sql.query_opt_mode = default;