import numpy as np

def detect_transition(data, positive_threshold=100):
    """
    Detect the transition point where the data goes from increasing near zero to a high positive value
    and then starts decreasing.
    
    Parameters:
    - data: list or numpy array of data points.
    - positive_threshold: the minimum value to detect an abrupt high positive value.
    
    Returns:
    - index: the index at which the transition occurs.
    """
    # Optional: Apply a simple moving average to smooth the data (window size = 3 for simplicity)
    smoothed_data = data
    
    for i in range(1, len(smoothed_data) - 1):
        # Detect transition where the value goes from increasing near zero to a high positive value and then starts decreasing
        if smoothed_data[i-1] <= 0 < smoothed_data[i] and smoothed_data[i] > positive_threshold and smoothed_data[i+1] < smoothed_data[i]:
            return i
    
    return None

# Provided data
data = [-100, -85, -76, -56, 0, 0,0, -34, -9, 0, 300, 278, 180]

# Set an appropriate positive threshold based on the data provided
positive_threshold = 100

transition_index = detect_transition(data, positive_threshold)
print(f"Transition detected at index: {transition_index}")
