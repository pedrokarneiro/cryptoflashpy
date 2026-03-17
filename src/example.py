'''
Accessing Data in the Python Code
=================================
Because of the mapping we adopted, our Python code inside the container
will always look "up" one level to find the data.

In our main.py, we would load a dataset like this:
'''
import pandas as pd
import os

# The path inside the container
data_path = os.path.join('..', 'data', 'raw', 'market_data.csv')

def load_data():
    if os.path.exists(data_path):
        df = pd.read_csv(data_path)
        print("Data loaded successfully!")
        return df
    else:
        print(f"File not found at {data_path}")

if __name__ == "__main__":
    load_data()

'''
Justification for this structure:
=================================
* Git Integrity: We can add /data/ to our .gitignore file so we don't accidentally push Gigabytes
of datasets to a repository, while still keeping our code in src/ version-controlled.
* Portability: If we move this project to a server later, we only need to move the data folder to
the same relative path and the Docker container will "just work."
* Safety: We can eventually mount the data folder as read-only (using - ./data:/app/data:ro)
if we want to ensure our Python scripts never accidentally overwrite our raw source data.
'''