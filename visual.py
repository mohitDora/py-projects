import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation

# Create a figure and axis
fig, ax = plt.subplots()
xdata, ydata = [], []
ln, = plt.plot([], [], 'b-', animated=True)

# Initialize the plot limits
def init():
    ax.set_xlim(0, 2 * np.pi)
    ax.set_ylim(-1, 1)
    return ln,

# Update the data for the animation
def update(frame):
    xdata.append(frame)
    ydata.append(np.sin(frame))
    ln.set_data(xdata, ydata)
    return ln,

# Generate frames
def data_gen():
    x = 0
    while True:
        yield x
        x += 0.1

# Create the animation
ani = animation.FuncAnimation(fig, update, frames=data_gen, init_func=init, blit=True, interval=100)

# Display the plot
plt.show()
