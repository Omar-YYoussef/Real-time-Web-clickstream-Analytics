from faker import Faker
import random
import pandas as pd

fake = Faker()

# Simulate user sessions
num_users = 1000
num_sessions_per_user = 10
data = []

for _ in range(num_users):
    for _ in range(num_sessions_per_user):
        user_id = fake.uuid4()
        session_start_time = fake.date_time_this_year()
        num_page_visits = random.randint(5, 20)
        
        for _ in range(num_page_visits):
            page_url = fake.uri_page()
            timestamp = fake.date_time_between(start_date=session_start_time, end_date='now')
            duration_on_page = random.randint(5, 300)  # in seconds
            interaction_type = random.choice(['click', 'scroll', 'exit', 'form_submit', None])
            
            data.append({
                'User ID': user_id,
                'Page URL': page_url,
                'Timestamp': timestamp,
                'Duration on Page (s)': duration_on_page,
                'Interaction Type': interaction_type
            })

# Convert data to DataFrame
clickstream_df = pd.DataFrame(data)
clickstream_df.to_csv('simulated_clickstream_data.csv', index=False)

##### V2 #####

from faker import Faker
import random
import pandas as pd

fake = Faker()

# Set the maximum number of rows
max_rows = 3000

data = []

while len(data) < max_rows:
    user_id = fake.uuid4()
    num_sessions_per_user = random.randint(1, 5)
    
    for _ in range(num_sessions_per_user):
        session_start_time = fake.date_time_this_year()
        num_page_visits = random.randint(5, 20)
        
        for _ in range(num_page_visits):
            page_url = fake.uri_page()
            timestamp = fake.date_time_between(start_date=session_start_time, end_date='now')
            duration_on_page = random.randint(5, 300)  # in seconds
            interaction_type = random.choice(['click', 'scroll', 'exit', 'form_submit', None])
            device_type = random.choice(['desktop', 'mobile', 'tablet'])
            browser = fake.user_agent()
            country = fake.country_code(representation="alpha-2")
            referrer = fake.uri()
            
            data.append({
                'User ID': user_id,
                'Page URL': page_url,
                'Timestamp': timestamp,
                'Duration on Page (s)': duration_on_page,
                'Interaction Type': interaction_type,
                'Device Type': device_type,
                'Browser': browser,
                'Country': country,
                'Referrer': referrer
            })
            
            if len(data) >= max_rows:
                break
            
        if len(data) >= max_rows:
            break
            
# Convert data to DataFrame
clickstream_df = pd.DataFrame(data[:max_rows])  # Truncate to max_rows if exceeded
clickstream_df.to_csv('Dataset V3.csv', index=False)

#### V3 ####

from faker import Faker
import random
import pandas as pd

fake = Faker()

# Set the maximum number of rows
max_rows = 15000

data = []

while len(data) < max_rows:
    user_id = fake.uuid4()
    session_start_time = fake.date_time_this_year()
    num_page_visits = random.randint(5, 20)
    pages_visited = set()  # To track unique pages visited in a session
    
    for _ in range(num_page_visits):
        available_pages = [fake.uri_page() for _ in range(100)]  # Generate a pool of 100 unique pages
        
        # Ensure unique page visits within a session without running an infinite loop
        for _ in range(100):  # Limit the attempts to find a unique page
            page_url = random.choice(available_pages)
            if page_url not in pages_visited:
                pages_visited.add(page_url)
                break
        
        timestamp = fake.date_time_between(start_date=session_start_time, end_date='now')
        duration_on_page = random.randint(5, 300)  # in seconds
        interaction_type = random.choice(['click', 'scroll', 'exit', 'form_submit'])
        device_type = random.choice(['desktop', 'mobile', 'tablet'])
        browser = fake.user_agent()
        country = fake.country_code(representation="alpha-2")
        referrer = fake.uri()
        
        data.append({
            'User ID': user_id,
            'Session Start Time': session_start_time,
            'Page URL': page_url,
            'Timestamp': timestamp,
            'Duration on Page (s)': duration_on_page,
            'Interaction Type': interaction_type,
            'Device Type': device_type,
            'Browser': browser,
            'Country': country,
            'Referrer': referrer
        })
        
        if len(data) >= max_rows:
            break
            
    if len(data) >= max_rows:
        break
            
# Convert data to DataFrame
clickstream_df = pd.DataFrame(data[:max_rows])  # Truncate to max_rows if exceeded
clickstream_df.to_csv('Dataset V3.csv', index=False)
