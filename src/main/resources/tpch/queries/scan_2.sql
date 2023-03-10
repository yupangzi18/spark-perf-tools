select
	l_orderkey,
	l_linenumber,
	l_extendedprice,
	l_discount
from
	ulineitem_2
where
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1' year
	and l_discount between .06 - 0.01 and .06 + 0.01
	and l_quantity < 24
