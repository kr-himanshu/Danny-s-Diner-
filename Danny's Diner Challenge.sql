 use dannys_diner;
 -- 1. What is the total amount each customer spent at the restaurant?
 select t1.customer_id,sum(price) as amt_spent from sales t1
 join menu t2 on t1.product_id=t2.product_id
 group by t1.customer_id;
 
 -- 2. How many days has each customer visited the restaurant?
 select customer_id,count(distinct order_date) as no_visit from sales
 group by customer_id;
 
 -- 3. What was the first item from the menu purchased by each customer?
 with cte as(select customer_id,product_name, row_number() over(partition by customer_id order by order_date) as rnk
 from menu t1
 join sales t2 on t1.product_id=t2.product_id) 
 
 select customer_id,product_name from cte 
 where rnk=1;
 
 -- alternate query
 
 select customer_id,product_name from sales t1
 join menu t2 on t1.product_id=t2.product_id
 group by customer_id
 order by min(order_date);
 
-- 4. What is the most purchased item on the menu and how many times was it purchased by all customers?
select product_name,count(*) as no_purchase from menu t1
join sales t2 on t1.product_id=t2.product_id
group by t1.product_id
order by no_purchase desc limit 1;

-- 5. Which item was the most popular for each customer?
WITH cte_popular_items AS (
  SELECT t1.customer_id,t2.product_name,COUNT(*) AS purchase_count,RANK() 
		OVER (PARTITION BY t1.customer_id ORDER BY COUNT(*) DESC) AS rnk
  FROM sales t1
  JOIN menu t2 ON t1.product_id = t2.product_id
  GROUP BY t1.customer_id, t2.product_name)
SELECT
  customer_id,product_name AS most_popular_item
FROM cte_popular_items
WHERE rnk = 1;
 
 
 -- 6.Which item was purchased first by the customer after they became a member?
 WITH cte_first_member_purchase AS (
    SELECT t1.customer_id AS customer,t3.product_name AS product,
    RANK() OVER (PARTITION BY t1.customer_id ORDER BY t2.order_date) AS rnk
    FROM members AS t1
	JOIN sales AS t2 ON t2.customer_id = t1.customer_id
	JOIN menu AS t3 ON t2.product_id = t3.product_id
    WHERE t2.order_date >= t1.join_date)
SELECT customer,product
FROM cte_first_member_purchase
WHERE rnk = 1;

-- 7. Which item was purchased just before the customer became a member?
WITH cte_last_member_purchase AS (
    SELECT t1.customer_id AS customer,t3.product_name AS product,
    RANK() OVER (PARTITION BY t1.customer_id ORDER BY t2.order_date desc) AS rnk
    FROM members AS t1
	JOIN sales AS t2 ON t2.customer_id = t1.customer_id
	JOIN menu AS t3 ON t2.product_id = t3.product_id
    WHERE t2.order_date < t1.join_date)
SELECT customer,product
FROM cte_last_member_purchase
WHERE rnk = 1;

-- 8. What is the total items and amount spent for each member before they became a member?
select t1.customer_id,sum(t3.price) as amt_spent,count(*) total_items from sales t1
join members t2 on t1.customer_id=t2.customer_id
join menu t3 on t1.product_id=t3.product_id
where t1.order_date<t2.join_date
group by t1.customer_id
order by t1.customer_id;

-- 9.If each $1 spent equates to 10 points and sushi has a 2x points multiplier 
-- how many points would each customer have?
WITH members_points AS (SELECT t1.customer_id,
        SUM(CASE
                WHEN t2.product_name = 'sushi' THEN (20 * t2.price) ELSE (10 * t2.price) END) AS members_points
    FROM sales t1
	JOIN menu t2 ON t1.product_id = t2.product_id
    GROUP BY t1.customer_id
)

SELECT
    customer_id,
    members_points
FROM
    members_points;

-- 10. In the first week after a customer joins the program (including their join date) 
-- they earn 2x points on all items, not just sushi - how many points do customer A and B have at the 
-- end of January?

WITH jan_points AS (
    SELECT
        t3.customer_id,
        SUM(
            CASE
                WHEN t2.order_date < t3.join_date THEN 
                    CASE
                        WHEN t1.product_name = 'sushi' THEN (20 * t1.price)
                        ELSE (10 * t1.price)
                    END
                WHEN DATEDIFF(t3.join_date, t2.order_date) > 6 THEN
                    CASE
                        WHEN t1.product_name = 'sushi' THEN (20 * t1.price)
                        ELSE (10 * t1.price)
                    END
                ELSE (t1.price * 20)
            END
        ) AS members_points
    FROM
        menu t1
        JOIN sales t2 ON t1.product_id = t2.product_id
        JOIN members t3 ON t2.customer_id = t3.customer_id
    WHERE
        t2.order_date <= '2021-01-31'
    GROUP BY
        t3.customer_id
)

SELECT
    customer_id,
    members_points
FROM
    jan_points;

-- bonus questions

-- Join All The Things
-- Recreate the table with: customer_id, order_date, product_name, price, member (Y/N)

select t1.customer_id,t1.order_date,t3.product_name,t3.price,
case when t1.order_date<t2.join_date then "N" 
	when t1.order_date>=t2.join_date then "Y" 
else "N" end as member
from sales t1
left join members t2 on t1.customer_id=t2.customer_id
left join menu t3 on t3.product_id=t1.product_id;


-- Rank All The Things
-- Recreate the table with: customer_id, order_date, product_name, price, member (Y/N), ranking(null/123)

with member_rnk as(
select t1.customer_id,t1.order_date,t3.product_name,t3.price,
case when t1.order_date<t2.join_date then "N" 
	when t1.order_date>=t2.join_date then "Y" 
else "N" end as member
from sales t1
left join members t2 on t1.customer_id=t2.customer_id
left join menu t3 on t3.product_id=t1.product_id
)

select *,
case when member="N" then null
else rank() over(partition by customer_id,member order by order_date ) end as ranking
  from member_rnk;