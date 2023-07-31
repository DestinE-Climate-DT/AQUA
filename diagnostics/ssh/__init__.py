"""ssh module"""

#To enable: from ssh import class sshVariability

from .ssh_class import sshVariability

__version__ = '0.0.1'

# This specifies which methods are exported publicly, used by "from ssh_class *"
__all__ = ["sshVariability"]