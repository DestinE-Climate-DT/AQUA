# Color Interpolator
# Given a sequence of N >= 2 colors ("the source"), 
# this script produces a sequence of any M >= 2 colors ("the target")
# with the first and last colors of the target identical to the source,
# and the colors in between equally spaced

# Author - Francois Massonnet (francois.massonnet@uclouvain.be / @FMassonnet)
# Date   - 12 Dec 2022

import matplotlib.pyplot as plt
import numpy as np

def colInterpolatOr(sourceColors, nTarget, colorCode = "RGB"):

  """ - sourceColors is a tuple of colors, by default in RGB format
      - nTarget is the desired number of output colors
      - colorCode : the format, either "RGB" (0-1) or "HEX"
  """

  if colorCode == "HEX":
    # From https://stackoverflow.com/questions/29643352/converting-hex-to-rgb-value-in-python
    sourceColors = [tuple(int(s[i:i + 2], 16) / 255. for i in (1, 3, 5)) for s in sourceColors] 
  elif colorCode != "RGB":
    print("ERROR: color code unknown")
  
  nSource = len(sourceColors)

  # List of output colors
  targetColors = list()


  # Let alpha be a scalar between 0 and 1, where 0 = first source color 
  # and 1 = last source color

  for alpha in np.linspace(0, 1, nTarget):

    # Identify the pair of source colors that will be used 
    # to make the linear interpolation. The variable "iRef" is the 
    # index of the first of these two source colors (iRef is between 0
    # and nSource - 1)

    iRef = int(np.floor(alpha * (nSource - 1)))

    # Reset last index to nSource - 1 since the last color is built 
    # from the last line
    if iRef == nSource - 1:
      iRef -= 1

    # Parameterize the corresponding line and fetch the value at alpha
    # beta is the parameter defining where (0 - 1) we are in between the two
    # source colors involved in the interpolation
    # beta is a simple scaling of alpha for the running interval

    beta = alpha * (nSource - 1) - iRef

    # Build the RGB triplet
    r = sourceColors[iRef][0] + beta * (sourceColors[iRef + 1][0] - sourceColors[iRef][0])
    g = sourceColors[iRef][1] + beta * (sourceColors[iRef + 1][1] - sourceColors[iRef][1])
    b = sourceColors[iRef][2] + beta * (sourceColors[iRef + 1][2] - sourceColors[iRef][2])


    
    if colorCode == "HEX":
      outputColor = '#%02x%02x%02x' % (int(r * 255), int(g * 255), int(b * 255))
    else:   
      outputColor = [r, g, b]

    targetColors.append(outputColor)
  

  return targetColors


# Example


def example():
  # List of colors that we want to pass through
  sourceColors = ["#1898e0", "#00b2ed", "#00bb62", \
               "#8bcd45", "#dbe622", "#f9c410", \
               "#f89e13", "#fb4c27", "#fb4865", \
               "#d24493", "#8f57bf", "#645ccc",]
  nSource = len(sourceColors)


  # Number of colors desired
  nTargets = [2, 16, 30]

  fig, ax = plt.subplots(len(nTargets) + 1, 1, figsize = (6, 6))

  for j in np.arange(nSource):
    xStart = j / nSource
    xEnd   = (j + 1) / nSource

    ax[0].fill_between((xStart, xEnd), (1, 1), color = sourceColors[j], edgecolor = "white")
    ax[0].set_title("Original colors (" + str(nSource) + ")")

  for f, n in enumerate(nTargets):
    targetColors = colInterpolatOr(sourceColors, colorCode = "HEX", nTarget = n)
  
    for j in np.arange(n):
      xStart = j / n
      xEnd   = (j + 1) / n

      ax[f + 1].fill_between((xStart, xEnd), (1, 1), color = targetColors[j], edgecolor = "white")
      ax[f + 1].set_title("Interpolated colors (" + str(n) + ")")


  for a in ax.flatten():
    a.set_xticklabels("")
    a.set_yticklabels("")
    a.set_xticks([])
    a.set_yticks([])
  plt.suptitle("https://github.com/fmassonn/snippets.git")
  

  figOut = "./colInterpolatOr.png"
  fig.tight_layout()
  fig.savefig(figOut, dpi = 300)
  print(figOut + " printed")


if __name__ == '__main__':
  example()
