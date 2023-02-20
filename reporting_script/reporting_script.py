import psycopg2
import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import messagebox

DB_HOST = "data-assignment.cvo9xh8ew9ac.eu-west-1.rds.amazonaws.com"
DB_NAME = "publitas_revolution_production"
USER = "assignment-user"
PASSOWRD = "JL34*$AyFzgQ"

# List for all tiers
lst = "'basic', 'bronze', 'enterprise', 'free', 'gold', 'gold-2021', 'professional', 'silver', 'Unknown'"

# Overview Query
def overview_query(start_date, end_date, tier):

    if tier == 'all':
        query_tiers = lst
    else:
        query_tiers = f"'{tier}'"

    query = f''' 
    WITH 
	dataset AS(
    -- Total Users -- 
    SELECT 
        'Total Users' AS category,  
        coalesce(z.tier, 'Unknown') AS tier,
        COUNT(*)
    FROM(
        SELECT
            DISTINCT 	
            a.id,
            c.tier
        FROM
            accounts AS a
        INNER JOIN 
            contracts AS c
        ON
            a.id = c.account_id) AS z
    GROUP BY 
        1,2
    
    UNION ALL
    
    -- Total Users Still Active --
    SELECT 
        'Total Live Users' AS category,  
        coalesce(z.tier, 'Unknown') AS tier,
        COUNT(*)
    FROM(
        SELECT
            DISTINCT 	
            a.id,
            c.tier
        FROM
            accounts AS a
        INNER JOIN 
            contracts AS c
        ON
            a.id = c.account_id
        WHERE
            a.deleted_at IS NULL) AS z
    GROUP BY 
        1,2
        
    UNION ALL	
        
    
    -- Total Users Lost --
    SELECT 
        'Total Lost Users' AS category,  
        coalesce(z.tier, 'Unknown') AS tier,
        COUNT(*)
    FROM(
        SELECT
            DISTINCT 	
            a.id,
            c.tier
        FROM
            accounts AS a
        INNER JOIN 
            contracts AS c
        ON
            a.id = c.account_id
        WHERE
            a.deleted_at IS NOT NULL) AS z
    GROUP BY 
        1,2
        
    UNION ALL
    
    -- Total Users Created In Range
    SELECT 
        'Total Created Users In Range' AS category,  
        coalesce(z.tier, 'Unknown') AS tier,
        COUNT(*)
    FROM(
        SELECT
            DISTINCT 	
            a.id,
            c.tier
        FROM
            accounts AS a
        INNER JOIN 
            contracts AS c
        ON
            a.id = c.account_id
        WHERE
            a.created_at BETWEEN DATE('{start_date}') AND DATE('{end_date}')) AS z
    GROUP BY 
        1,2
        
    UNION ALL 
        
    -- Total Users Lost In Range
    SELECT 
        'Total Lost Users In range' AS category,  
        coalesce(z.tier, 'Unknown') AS tier,
        COUNT(*)
    FROM(
        SELECT
            DISTINCT 	
            a.id,
            c.tier
        FROM
            accounts AS a
        INNER JOIN 
            contracts AS c
        ON
            a.id = c.account_id
        WHERE
            a.deleted_at BETWEEN DATE('{start_date}') AND DATE('{end_date}')) AS z
    GROUP BY 
        1,2),
        
        dataset2 AS (
            SELECT
                *
            FROM
                publications
            WHERE
                first_online_at BETWEEN DATE('{start_date}') AND DATE('{end_date}')),
                
        publication_count AS(
        SELECT 
            coalesce(c.tier,  'Unknown') AS tier,
            COUNT(*) AS publication_count
        FROM
            dataset2
        INNER JOIN 
            groups AS g
        ON
            dataset2.group_id = g.id
        INNER JOIN 
            contracts AS c
        ON
            g.account_id = c.account_id
        INNER JOIN 
            accounts AS a
        ON 
            c.account_id = a.id
        GROUP BY 
            1),
    
        active_users AS(
            SELECT 
                DISTINCT
                a.id,
                coalesce(c.tier, 'Unknown') AS tier
            FROM
                dataset2
            INNER JOIN 
                groups AS g
            ON
                dataset2.group_id = g.id
            INNER JOIN 
                contracts AS c
            ON
                g.account_id = c.account_id
            INNER JOIN 
                accounts AS a
            ON 
                c.account_id = a.id),
    
            active_user_count AS (
            SELECT 
            tier,
            COUNT(*) AS cnt
        FROM
            active_users
        GROUP BY 
            1
        ORDER BY 	
            1),
            
    -- Dataset for quartile calculation for each tier in timefame
        quartile_dataset AS (
            SELECT 
                coalesce(c.tier, 'Unknown') AS tier,
                a.id, 
                COUNT(*) AS publication_count
            FROM
                dataset2
            INNER JOIN 
                groups AS g
            ON
                dataset2.group_id = g.id
            INNER JOIN 
                contracts AS c
            ON
                g.account_id = c.account_id
            INNER JOIN 
                accounts AS a
            ON 
                c.account_id = a.id
            GROUP BY 
                1,
                2),
            
    -- Datset for upper and lower bound of publication count by teir for outliers based on timeframe		
            bd_dataset AS(
                SELECT 
                  coalesce(tier, 'Unknown') AS tier, 
                  (SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY publication_count)
                   FROM quartile_dataset
                   WHERE coalesce(tier, 'Unknown') = coalesce(t.tier, 'Unknown')) AS q1,
                  (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY publication_count)
                   FROM quartile_dataset
                   WHERE coalesce(tier, 'Unknown') = coalesce(t.tier, 'Unknown')) AS q3,
                  ROUND((SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY publication_count)
                   FROM quartile_dataset
                   WHERE coalesce(tier, 'Unknown') = coalesce(t.tier, 'Unknown')) * 1.5) AS upper_bound
                FROM quartile_dataset t
                GROUP BY tier),
                
            bounds_dataset AS (
                SELECT 
                    tier,
                    q3 + (1.5 * (q3 - q1)) AS upper_bound
                FROM
                    bd_dataset)
    
    SELECT 
        dataset.tier,
        SUM(CASE WHEN category = 'Total Users' THEN count ELSE 0 END) AS historic_total_users, 
        SUM(CASE WHEN category = 'Total Live Users' THEN count ELSE 0 END) AS current_total_live_users, 
        SUM(CASE WHEN category = 'Total Lost Users' THEN count ELSE 0 END) AS historic_total_lost_users, 
        SUM(CASE WHEN category = 'Total Created Users In Range' THEN count ELSE 0 END) AS total_users_new_in_range, 
        SUM(CASE WHEN category = 'Total Lost Users In range' THEN count ELSE 0 END) AS total_users_lost_in_range, 
        SUM(CASE WHEN category = 'Total Created Users In Range' THEN count ELSE 0 END) - SUM(CASE WHEN category = 'Total Lost Users In range' THEN count ELSE 0 END) total_net_new_in_range,
        publication_count.publication_count AS total_publications_in_range,
        ROUND(publication_count.publication_count /(SELECT SUM(publication_count) FROM publication_count),4) AS perc_of_total_publications,
        active_user_count.cnt AS active_users_in_range,
        ROUND(active_user_count.cnt/SUM(CASE WHEN category = 'Total Live Users' THEN count ELSE 0 END),4) AS perc_live_user_base,
        ROUND(active_user_count.cnt/(SELECT SUM(cnt) FROM active_user_count),4) AS perc_active_user_base,
        ROUND(publication_count.publication_count / active_user_count.cnt) AS avg_pulications_per_active_user,
        ROUND(bounds_dataset.upper_bound) AS tier_pc_upper_bound
        
    FROM
        dataset
    
    LEFT JOIN 
        publication_count 
    ON 
        coalesce(dataset.tier, 'Unkown') = coalesce(publication_count.tier, 'Unkown')
    
    LEFT JOIN 
        active_user_count
    ON
        coalesce(dataset.tier, 'Unkown') = coalesce(active_user_count.tier, 'Unkown')
    
    LEFT JOIN 
        bounds_dataset
    ON
        coalesce(dataset.tier, 'Unkown') = coalesce(bounds_dataset.tier, 'Unkown')
        
    GROUP BY 
        dataset.tier,
        publication_count,
        active_users_in_range,
        tier_pc_upper_bound

	HAVING 
		coalesce(dataset.tier, 'Unkown') IN ({query_tiers})        
        
    ORDER BY 
        1'''
    print('query_captured')
    return query


# Monthly Query
def monthly_query(start_date, end_date, tier):

    if tier == 'all':
        query_tiers = lst
    else:
        query_tiers = f"'{tier}'"


    query = f'''
    WITH 
	dataset AS(
	
		SELECT
			'users' AS category, 
			EXTRACT(YEAR FROM p.first_online_at) AS yr,
			EXTRACT(MONTH FROM p.first_online_at) AS mth,
		    coalesce(c.tier, 'Unknown') AS tier,
			COUNT(name) AS count
		FROM
			publications AS p
		INNER JOIN 
			groups AS g
		ON
			p.group_id = g.id
		INNER JOIN 
			accounts AS a
		ON 
			a.id = g.account_id
		INNER JOIN 
			contracts AS c
		ON
			c.account_id = a.id
		WHERE 
			p.first_online_at BETWEEN DATE('2021-01-01') AND DATE('2022-12-31')
		GROUP BY
			1,2,3,4
		
		UNION ALL	
	
	
	
    -- Total Users Created In Range
        SELECT 
            'Total Created Users In Range' AS category, 
            z.yr,
            z.mth,
            coalesce(z.tier, 'Unknown') AS tier,
            COUNT(*)
        FROM(
        SELECT
            DISTINCT
            EXTRACT(YEAR FROM a.created_at) AS yr,
            EXTRACT(MONTH FROM a.created_at) AS mth,
            a.id,
            c.tier
        FROM
            accounts AS a
        INNER JOIN 
            contracts AS c
        ON
            a.id = c.account_id
        WHERE
            a.created_at BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        ORDER BY 
            1,2,3,4) AS z
        GROUP BY 
            1,2,3,4
    
        UNION ALL 
    
        -- Total Users Lost In Range
        SELECT 
            'Total Lost Users In range' AS category,
            z.yr,
            z.mth,
            coalesce(z.tier, 'Unknown') AS tier,
            COUNT(*)
        FROM(
            SELECT
                DISTINCT
                EXTRACT(YEAR FROM a.deleted_at) AS yr,
                EXTRACT(MONTH FROM a.deleted_at) AS mth,
                a.id,
                c.tier
            FROM
                accounts AS a
            INNER JOIN 
                contracts AS c
            ON
                a.id = c.account_id
            WHERE
                a.deleted_at BETWEEN DATE('{start_date}') AND DATE('{end_date}')) AS z
        GROUP BY 
            1,2,3,4),
    
        dataset2 AS (
            SELECT
                *
            FROM
                publications
            WHERE
                first_online_at BETWEEN DATE('{start_date}') AND DATE('{end_date}')),
                
        publication_count AS(
        SELECT
            EXTRACT(YEAR FROM dataset2.first_online_at) AS yr,
            EXTRACT(MONTH FROM dataset2.first_online_at) AS mth,
            coalesce(c.tier,  'Unknown') AS tier,
            COUNT(*) AS publication_count
        FROM
            dataset2
        INNER JOIN 
            groups AS g
        ON
            dataset2.group_id = g.id
        INNER JOIN 
            contracts AS c
        ON
            g.account_id = c.account_id
        INNER JOIN 
            accounts AS a
        ON 
            c.account_id = a.id
        GROUP BY 
            1,2,3),
    
        active_users AS(
            SELECT 
                DISTINCT
                EXTRACT(YEAR FROM dataset2.first_online_at) AS yr,
                EXTRACT(MONTH FROM dataset2.first_online_at) AS mth,
                a.id,
                coalesce(c.tier, 'Unknown') AS tier
            FROM
                dataset2
            INNER JOIN 
                groups AS g
            ON
                dataset2.group_id = g.id
            INNER JOIN 
                contracts AS c
            ON
                g.account_id = c.account_id
            INNER JOIN 
                accounts AS a
            ON 
                c.account_id = a.id),
    
            active_user_count AS (
            SELECT 
            yr,
            mth,
            tier,
            COUNT(*) AS cnt
        FROM
            active_users
        GROUP BY 
            1,2,3
        ORDER BY 	
            1),
    
    -- Dataset for quartile calculation for each tier in timefame
        quartile_dataset AS (
            SELECT
                EXTRACT(YEAR FROM dataset2.first_online_at) AS yr,
                EXTRACT(MONTH FROM dataset2.first_online_at) AS mth,
                coalesce(c.tier, 'Unknown') AS tier,
                a.id, 
                COUNT(*) AS publication_count
            FROM
                dataset2
            INNER JOIN 
                groups AS g
            ON
                dataset2.group_id = g.id
            INNER JOIN 
                contracts AS c
            ON
                g.account_id = c.account_id
            INNER JOIN 
                accounts AS a
            ON 
                c.account_id = a.id
            GROUP BY 
                1,
                2,3,4),
            
    -- Datset for upper and lower bound of publication count by teir for outliers based on timeframe		
            bd_dataset AS(
                SELECT 
                  yr,
                  mth,
                  coalesce(tier, 'Unknown') AS tier, 
                  (SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY publication_count)
                   FROM quartile_dataset
                   WHERE 
                    yr = t.yr
                    AND mth = t.mth
                    AND coalesce(tier, 'Unknown') = coalesce(t.tier, 'Unknown')) AS q1,
                  (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY publication_count)
                   FROM quartile_dataset
                   WHERE 
                    yr = t.yr
                    AND mth = t.mth
                    AND coalesce(tier, 'Unknown') = coalesce(t.tier, 'Unknown')) AS q3,
                  ROUND((SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY publication_count)
                   FROM quartile_dataset
                   WHERE
                    yr = t.yr
                    AND mth = t.mth
                    AND coalesce(tier, 'Unknown') = coalesce(t.tier, 'Unknown')) * 1.5) AS upper_bound
                FROM quartile_dataset t
                GROUP BY yr, mth, tier),
    
            bounds_dataset AS (
                SELECT 
                    yr,
                    mth,
                    tier,
                    q3 + (1.5 * (q3 - q1)) AS upper_bound
                FROM
                    bd_dataset)
            
    SELECT 
        dataset.yr,
        dataset.mth,
        dataset.tier,
        SUM(CASE WHEN category = 'Total Created Users In Range' THEN count ELSE 0 END) AS total_users_new_in_range, 
        SUM(CASE WHEN category = 'Total Lost Users In range' THEN count ELSE 0 END) AS total_users_lost_in_range, 
        SUM(CASE WHEN category = 'Total Created Users In Range' THEN count ELSE 0 END) - SUM(CASE WHEN category = 'Total Lost Users In range' THEN count ELSE 0 END) total_net_new_in_range,
        publication_count.publication_count AS total_publications_in_range,
        ROUND(publication_count.publication_count /(SELECT SUM(publication_count) FROM publication_count WHERE yr = dataset.yr AND mth = dataset.mth),4) AS perc_of_total_publications,
        active_user_count.cnt AS active_users_in_range,
        ROUND(active_user_count.cnt/(SELECT SUM(cnt) FROM active_user_count WHERE yr = dataset.yr AND mth = dataset.mth),4) AS perc_active_user_base,
        ROUND(publication_count.publication_count / active_user_count.cnt) AS avg_pulications_per_active_user,
        ROUND(bounds_dataset.upper_bound) AS tier_pc_upper_bound
        
    FROM
        dataset
    
    LEFT JOIN 
        publication_count 
    ON
        dataset.yr = publication_count.yr
        AND dataset.mth = publication_count.mth
        AND coalesce(dataset.tier, 'Unkown') = coalesce(publication_count.tier, 'Unkown')
        
    LEFT JOIN 
        active_user_count
    ON
        dataset.yr = active_user_count.yr
        AND dataset.mth = active_user_count.mth
        AND coalesce(dataset.tier, 'Unkown') = coalesce(active_user_count.tier, 'Unkown')
    
    LEFT JOIN 
        bounds_dataset
    ON
        dataset.yr = bounds_dataset.yr
        AND dataset.mth = bounds_dataset.mth
        AND coalesce(dataset.tier, 'Unkown') = coalesce(bounds_dataset.tier, 'Unkown')
    
    GROUP BY 
        1,2,3,total_publications_in_range, active_users_in_range, tier_pc_upper_bound
        
	HAVING 
		coalesce(dataset.tier, 'Unkown') IN ({query_tiers})       
        
    ORDER BY
        1,2,3,total_publications_in_range
    '''
    return query


def my_function(password, user, db_name, db_host, start_date, end_date, tier):
    # Do something with the user inputs

    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=user,
        password=password
    )

    def query_function(query, filename):
        try:
            # Try to connect to the database
            conn = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=user,
                password=password
            )
            print("Successfully connected to the database.")

            cur = conn.cursor()

            # Execute a SELECT query on the accounts table
            cur.execute(query)

            # Fetch all the rows returned by the query
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

            # Save the results to a CSV file
            df.to_csv(filename, index=False)

            # Close the cursor and database connections
            cur.close()
            conn.close()


        except Exception as e:
            # If there's an error, print out the error message
            print("Error: Unable to connect to the database.")
            print(e)

        conn.close()


    try:


        q1 = overview_query(start_date, end_date, tier)
        q2 = monthly_query(start_date, end_date, tier)

        q1_file_name = f'range_overview_report_{tier}_{start_date}_{end_date}.csv'
        q2_file_name = f'range_breakdown_report_{tier}_{start_date}_{end_date}.csv'

        query_function(q1, q1_file_name)
        query_function(q2, q2_file_name)


        print(f"Password: {password}\nUser: {user}\nDatabase Name: {db_name}\nDatabase Host: {db_host}")
        print(f"Start Date: {start_date}\nEnd Date: {end_date}\nTier: {tier}")
        messagebox.showinfo("Submission Successful", "Your form has been submitted successfully!")
    except:
        messagebox.showerror("Submission Failed", "There was an error with your submission.")






# Create the GUI
root = tk.Tk()
root.title("Monhtly Reporting Application")

# Create the heading
heading_label = tk.Label(root, text="Monthly Reporting Application", font=("Arial", 16), padx=10, pady=10)
heading_label.grid(row=0, column=0, columnspan=2)

# Create the info section
info_label = tk.Label(root, text="Please enter the following information:", font=("Arial", 12), padx=10, pady=10)
info_label.grid(row=1, column=0, columnspan=2)

# Create the database host input
db_host_label = tk.Label(root, text="Database Host:")
db_host_label.grid(row=2, column=0)
db_host_entry = tk.Entry(root)
db_host_entry.grid(row=2, column=1)
db_host_entry.insert(0, DB_HOST)  # pre-populate with a value

# Create the database name input
db_name_label = tk.Label(root, text="Database Name:")
db_name_label.grid(row=3, column=0)
db_name_entry = tk.Entry(root)
db_name_entry.grid(row=3, column=1)
db_name_entry.insert(0, DB_NAME)  # pre-populate with a value

# Create the user input
user_label = tk.Label(root, text="User:")
user_label.grid(row=4, column=0)
user_entry = tk.Entry(root)
user_entry.grid(row=4, column=1)
user_entry.insert(0, USER)  # pre-populate with a value

# Create the password input
password_label = tk.Label(root, text="Password:")
password_label.grid(row=5, column=0)
password_entry = tk.Entry(root, show="*")
password_entry.grid(row=5, column=1)
password_entry.insert(0, PASSOWRD)  # pre-populate with a value


# Create the start date input
start_date_label = tk.Label(root, text="Start Date (YYYY-MM-DD):")
start_date_label.grid(row=6, column=0)
start_date_entry = tk.Entry(root)
start_date_entry.grid(row=6, column=1)
start_date_entry.insert(0, "2021-01-01")  # pre-populate with a value

# Create the end date input
end_date_label = tk.Label(root, text="End Date (YYYY-MM-DD):")
end_date_label.grid(row=7, column=0)
end_date_entry = tk.Entry(root)
end_date_entry.grid(row=7, column=1)
end_date_entry.insert(0, "2022-12-31")

# Create the tier input
tier_label = tk.Label(root, text="Tier:")
tier_label.grid(row=8, column=0, padx=5, pady=5)
tier_options = ["all", "basic", "bronze", "enterprise", "free", "gold", "gold-2021", "professional", "silver", "Unknown"]
tier_variable = tk.StringVar(root)
tier_variable.set(tier_options[0])
tier_dropdown = tk.OptionMenu(root, tier_variable, *tier_options)
tier_dropdown.grid(row=8, column=1, padx=5, pady=5)


# Create the button to submit the inputs
submit_button = tk.Button(root, text="Submit", command=lambda: my_function(password_entry.get(), user_entry.get(), db_name_entry.get(), db_host_entry.get(), start_date_entry.get(), end_date_entry.get(), tier_variable.get()))
submit_button.grid(row=10, column=1, padx=5, pady=5)

# Start the GUI
root.mainloop()
