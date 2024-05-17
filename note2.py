import pandas as pd
import random

# Define Indian states and their corresponding city lists
states = {
    "Andhra Pradesh": ["Visakhapatnam", "Guntur", "Vijayawada", "Hyderabad", "Tirupati"],
    "Arunachal Pradesh": ["Itanagar", "Pasighat", "Tezu"],
    "Assam": ["Guwahati", "Dibrugarh", "Silchar", "Jorhat"],
    "Bihar": ["Patna", "Gaya", "Muzaffarpur", "Bhagalpur"],
    "Chhattisgarh": ["Raipur", "Bilaspur", "Durg", "Korba"],
    # ... (repeat for all Indian states)
}

# Function to randomly select a city from a state's list
def get_random_city(state):
    return random.choice(states[state])

# Define number of data rows
num_rows = 600

# Generate data
data = []
for i in range(num_rows):
    random_state = random.choice(list(states.keys()))
    random_city = get_random_city(random_state)

    # Generate random data for each column
    state_code = random.randint(1, 35)  # Adjust range for actual number of states
    dist_code = random.randint(1, 100)
    population = random.randint(100000, 1000000)
    male_population = random.randint(int(population * 0.49), int(population * 0.51))
    female_population = population - male_population
    literate = random.randint(int(population * 0.6), int(population * 0.8))
    male_literate = random.randint(int(literate * 0.49), int(literate * 0.51))
    female_literate = literate - male_literate  # Assuming typo, corrected to Female_Literate
    sex_ratio = random.randint(900, 1100)
    child_sex_ratio = random.randint(900, 1100)
    graduates = random.randint(int(population * 0.1), int(population * 0.2))
    male_graduates = random.randint(int(graduates * 0.49), int(graduates * 0.51))
    female_graduates = graduates - male_graduates

    row = {
        "City": random_city,
        "State_Code": state_code,
        "State_Name": random_state,
        "Dist_Code": dist_code,
        "Total_Population": population,
        "Male_population": male_population,
        "Female_Population": female_population,
        "Total_Literate": literate,
        "Male_Literate": male_literate,
        "Female_Literate": female_literate,
        "Sex_ratio": sex_ratio,
        "Child_sex": child_sex_ratio,
        "Total_Graduate": graduates,
        "Male_Graduate": male_graduates,
        "Female_Graduate": female_graduates,
    }
    data.append(row)

# Create DataFrame and save to CSV
census_data = pd.DataFrame(data)
census_data.to_csv("India_city_data.csv", index=False)

print("Indian city and state data saved to India_city_data.csv")
