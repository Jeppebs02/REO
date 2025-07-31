import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

solar_extra_cap = 1000

file_path ="C:\\Users\\jeppe\\Documents\\GitHub\\REO\\ENTSO-E\\numpy_binaries\\ny\\Horns_Rev_C_2025-01-01_to_2025-07-30.npy"

Horns_Rev_C=np.load(file_path, allow_pickle=True)

# Hvis det er en liste af arrays/dicts, konverter til DataFrame
df = pd.DataFrame(Horns_Rev_C)

# Gem til CSV for nem deling og fremtidig læsning
df.to_csv("Horns_Rev_C_2025.csv", index=False)